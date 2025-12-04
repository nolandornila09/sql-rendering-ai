const fs = require("fs");
const path = require("path");

// -----------------------------------------------------------------------------
// Helper: Extract SQL action keywords (SELECT, INSERT, etc.)
// -----------------------------------------------------------------------------
function extractActions(sqlText, filterList = null) {
  const actionPattern = /\b(SELECT|WITH|DELETE|INSERT|UPDATE|DROP|ALTER|CREATE|TRUNCATE)\b/gi;
  const actions = [...new Set((sqlText.match(actionPattern) || []).map(a => a.toUpperCase()))];
  return filterList ? actions.filter(a => filterList.includes(a)) : actions;
}

// ==================== TEMPORAL FILTER CONFIGURATION ====================

// Valid date column suffixes
const DATE_COLUMN_SUFFIXES = ['_date', '_at', '_time', '_timestamp'];

// Valid temporal parameters (in addition to date-named parameters)
const WINDOW_PARAMETERS = ['window_days', 'lookback_days', 'offset_days', 'days_back', 'period_days'];

// Temporal operators
const TEMPORAL_OPERATORS = ['BETWEEN', 'IN', '=', '>=', '<=', '>', '<'];

/**
 * Detects if a column name follows date naming conventions
 */
function isDateColumn(columnName) {
  const lower = columnName.toLowerCase();
  return DATE_COLUMN_SUFFIXES.some(suffix => lower.endsWith(suffix));
}

/**
 * Detects if a parameter name is date-related
 */
function isDateParameter(paramName) {
  const lower = paramName.toLowerCase();
  // Check if it's a date-named parameter or a window parameter
  return DATE_COLUMN_SUFFIXES.some(suffix => lower.endsWith(suffix)) ||
         WINDOW_PARAMETERS.some(window => lower.includes(window));
}

/**
 * SIMPLIFIED: Validates whether SQL contains a proper temporal date filter.
 * Just checks for presence of key components rather than parsing exact structure.
 * Returns: { valid: boolean, reason: string, found: { dateColumns, dateParameters, operators, hasWhereClause } }
 */
function hasValidTemporalFilter(sqlText) {
  const sqlWithoutComments = sqlText.replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');
  
  // Step 1: Check for WHERE clause
  const hasWhereClause = /\bWHERE\b/i.test(sqlWithoutComments);
  
  // Step 2: Find date columns
  const dateColumnPattern = new RegExp(`\\b(\\w+(?:${DATE_COLUMN_SUFFIXES.join('|')}))\\b`, 'gi');
  const dateColumnsFound = [...new Set((sqlWithoutComments.match(dateColumnPattern) || []).map(c => c.toLowerCase()))];
  
  // Step 3: Find date parameters
  const dateParamPattern = new RegExp(
    `@(\\w*(?:${DATE_COLUMN_SUFFIXES.join('|').replace(/_/g, '_?')})|` +
    `\\w*(?:${WINDOW_PARAMETERS.join('|')}))\\b`,
    'gi'
  );
  const dateParametersFound = [...new Set((sqlWithoutComments.match(dateParamPattern) || []).map(p => p.toLowerCase()))];
  
  // Step 4: Check for temporal operators
  const operatorPattern = /\b(BETWEEN|IN)\b|([><=]+)/gi;
  const operatorsFound = [...new Set((sqlWithoutComments.match(operatorPattern) || []).map(o => o.toUpperCase()))];
  
  // Build result
  const found = {
    dateColumns: dateColumnsFound,
    dateParameters: dateParametersFound,
    operators: operatorsFound,
    hasWhereClause
  };
  
  // Validation logic
  if (!hasWhereClause) {
    return {
      valid: false,
      reason: "No WHERE clause found",
      found
    };
  }
  
  if (dateColumnsFound.length === 0) {
    return {
      valid: false,
      reason: "No date columns found (must end with: _date, _at, _time, _timestamp)",
      found
    };
  }
  
  if (dateParametersFound.length === 0) {
    return {
      valid: false,
      reason: "No date parameters found (e.g., @as_of_date, @start_date, @window_days)",
      found
    };
  }
  
  if (operatorsFound.length === 0) {
    return {
      valid: false,
      reason: "No temporal operators found (BETWEEN, IN, =, >=, <=, >, <)",
      found
    };
  }
  
  // Optional: Check that date column and date parameter appear near each other
  // This helps ensure they're actually used together
  const whereClauseMatch = sqlWithoutComments.match(/WHERE[\s\S]*/i);
  if (whereClauseMatch) {
    const whereClause = whereClauseMatch[0];
    const hasDateColumnInWhere = dateColumnsFound.some(col => 
      new RegExp(`\\b${col}\\b`, 'i').test(whereClause)
    );
    const hasDateParamInWhere = dateParametersFound.some(param => 
      new RegExp(param.replace('@', '@'), 'i').test(whereClause)
    );
    
    if (!hasDateColumnInWhere) {
      return {
        valid: false,
        reason: "Date column not used in WHERE clause",
        found
      };
    }
    
    if (!hasDateParamInWhere) {
      return {
        valid: false,
        reason: "Date parameter not used in WHERE clause",
        found
      };
    }
  }
  
  // All checks passed!
  return {
    valid: true,
    reason: "Valid temporal filter found",
    found
  };
}

// Load policy
const policyData = JSON.parse(
  fs.readFileSync(path.join(__dirname, "../../intents/policy.json"), "utf8")
);

// Normalize policy structure to handle both flat and nested formats
const policy = {
  rules: policyData.rules.query_behavior 
    ? {
        // Enhanced format with nested structure
        allowed_actions: policyData.rules.query_behavior.allowed_actions,
        disallowed_actions: policyData.rules.query_behavior.disallowed_actions,
        row_limit: policyData.rules.query_behavior.row_limit,
        max_window_days: policyData.rules.query_behavior.max_window_days,
        disallow_future_as_of: policyData.rules.query_behavior.disallow_future_as_of,
        ...policyData.rules
      }
    : {
        // Simple flat format
        ...policyData.rules
      }
};

// Discover all SQL templates
const templatesDir = path.join(__dirname, "../../intents");

// Check if directory exists
if (!fs.existsSync(templatesDir)) {
  console.error(`Templates directory not found: ${templatesDir}`);
  console.error(`   Current directory: ${__dirname}`);
  process.exit(1);
}

const templateFiles = fs.readdirSync(templatesDir)
  .filter(file => file.endsWith('.sql.tmpl'));

if (templateFiles.length === 0) {
  console.warn(`Warning: No SQL template files found in: ${templatesDir}`);
}

console.log(`\nFound ${templateFiles.length} SQL template(s) to validate`);
console.log(`Policy rules loaded: ${Object.keys(policy.rules).length} categories\n`);

// ==================== EXEMPTIONS ====================

// Test files that are intentionally non-compliant for testing purposes
const EXEMPT_TEST_FILES = [
  'test_truncate.sql.tmpl',
  'test_invalid.sql.tmpl',
  'test_security_violation.sql.tmpl'
];

// ==================== INDIVIDUAL TEMPLATE TESTS ====================

templateFiles.forEach(templateFile => {
  describe(`Policy Guardrails: ${templateFile}`, () => {
    let sql;
    let isExempt;
    
    beforeAll(() => {
      const templatePath = path.join(templatesDir, templateFile);
      sql = fs.readFileSync(templatePath, "utf8");
      isExempt = EXEMPT_TEST_FILES.includes(templateFile);
      
      if (isExempt) {
        console.log(`   ${templateFile} is marked as EXEMPT (test file)`);
      }
    });

    // ==================== MANDATORY TEMPORAL FILTER (SIMPLIFIED) ====================
    
    describe("Mandatory Temporal Date Filter", () => {
      test(`Must include temporal date filter with proper naming conventions`, () => {
        if (isExempt) {
          console.log(`   Skipping temporal filter check for exempt file: ${templateFile}`);
          return;
        }
        
        const result = hasValidTemporalFilter(sql);
        
        if (!result.valid) {
          throw new Error(
            `${templateFile} is missing required temporal date filter.\n` +
            `   Reason: ${result.reason}\n` +
            `   Found: ${JSON.stringify(result.found, null, 2)}\n\n` +
            `   Requirements:\n` +
            `   - Must have WHERE clause\n` +
            `   - Column must end with: ${DATE_COLUMN_SUFFIXES.join(', ')}\n` +
            `   - Must use operators: ${TEMPORAL_OPERATORS.join(', ')}\n` +
            `   - Parameters must be date-named (e.g., @as_of_date, @start_date) or window params (e.g., @window_days)\n` +
            `   - Date column and date parameter must appear together in WHERE clause\n\n` +
            `   Examples:\n` +
            `   ✓ WHERE ... order_date BETWEEN @start_date AND @end_date\n` +
            `   ✓ WHERE ... as_of_date = @as_of_date\n` +
            `   ✓ WHERE ... as_of_date IN (@date1, @date2)\n` +
            `   ✓ WHERE ... CAST(order_date AS DATE) BETWEEN DATEADD(DAY, -@window_days, @as_of_date) AND @as_of_date`
          );
        }
        
        // Log found filters for visibility
        if (result.found.dateColumns && result.found.dateColumns.length > 1) {
          console.log(`   Found ${result.found.dateColumns.length} date columns in ${templateFile}: ${result.found.dateColumns.join(', ')}`);
        }
        
        expect(result.valid).toBe(true);
      });

      test(`Temporal filter must use parameterized values (no hardcoded dates)`, () => {
        if (isExempt) {
          console.log(`   Skipping hardcoded date check for exempt file: ${templateFile}`);
          return;
        }
        
        const sqlWithoutComments = sql.replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');
        
        // Detect date columns being filtered with literal values
        const literalDatePatterns = [
          { pattern: /(\w+_(?:date|at|time|timestamp))\s*=\s*'[\d-]+'/i, name: "equality with literal" },
          { pattern: /(\w+_(?:date|at|time|timestamp))\s*BETWEEN\s*'[\d-]+'/i, name: "BETWEEN with literal" },
          { pattern: /(\w+_(?:date|at|time|timestamp))\s*IN\s*\([^@]*'[\d-]+'/i, name: "IN with literal" }
        ];
        
        const violations = [];
        literalDatePatterns.forEach(({ pattern, name }) => {
          const match = pattern.exec(sqlWithoutComments);
          if (match) {
            violations.push(`${match[1]} uses ${name}`);
          }
        });
        
        if (violations.length > 0) {
          throw new Error(
            `${templateFile} has temporal filters with hardcoded values:\n` +
            violations.map(v => `   - ${v}`).join('\n') + '\n' +
            `   All date filters must use parameterized values (e.g., @as_of_date, @start_date)`
          );
        }
        
        expect(violations).toHaveLength(0);
      });

      test(`Temporal filter parameters must follow naming conventions`, () => {
        if (isExempt) {
          console.log(`   Skipping parameter naming check for exempt file: ${templateFile}`);
          return;
        }
        
        const result = hasValidTemporalFilter(sql);
        
        // This is already validated by hasValidTemporalFilter
        // Just ensure the check passed
        expect(result.valid).toBe(true);
      });
    });

    // ==================== SECURITY REQUIREMENTS (UNCHANGED) ====================
    
    describe("Security Requirements", () => {
      test(`Must include tenant_id filter`, () => {
        if (isExempt) {
          console.log(`   Skipping tenant_id check for exempt file: ${templateFile}`);
          return;
        }
        
        const tenantIdPattern = /WHERE[\s\S]*?tenant_id\s*=\s*[@$:][a-zA-Z_][a-zA-Z0-9_]*/i;
        
        if (!tenantIdPattern.test(sql)) {
          throw new Error(
            `${templateFile} is missing required tenant_id filter.\n` +
            `   CRITICAL SECURITY: Every query must filter by tenant_id to prevent cross-tenant data access.`
          );
        }
        
        // Ensure it's not just in comments
        const sqlWithoutComments = sql.replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');
        expect(sqlWithoutComments).toMatch(tenantIdPattern);
      });

      test(`Must not use SELECT *`, () => {
        const selectAllPattern = /SELECT\s+\*(?!\s*FROM\s*\()/i;
        const sqlWithoutComments = sql.replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');
        
        if (selectAllPattern.test(sqlWithoutComments)) {
          throw new Error(
            `${templateFile} uses SELECT * which is not allowed.\n` +
            `   Security & Performance: Explicitly list all required columns.`
          );
        }
        
        expect(sqlWithoutComments).not.toMatch(selectAllPattern);
      });

      test(`Must not contain JOIN`, () => {
        // JOINs are completely disallowed
        const joinPattern = /\bJOIN\b/i;
        const sqlWithoutComments = sql.replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');
        
        if (joinPattern.test(sqlWithoutComments)) {
          throw new Error(
            `${templateFile} contains JOIN which is not allowed.\n` +
            `   POLICY: JOINs are prohibited. Use separate queries or pre-joined views instead.`
          );
        }
        
        expect(sqlWithoutComments).not.toMatch(joinPattern);
      });

      test(`Must use parameterized queries`, () => {
        const sqlInjectionPatterns = [
          { pattern: /'\s*\+\s*@/, name: "String concatenation with parameters" },
          { pattern: /\|\|.*@/, name: "String concatenation (PostgreSQL/Oracle)" },
          { pattern: /CONCAT\s*\(\s*'[^']*'\s*,\s*@/i, name: "CONCAT with mixed literals and parameters" }
        ];
        
        sqlInjectionPatterns.forEach(({ pattern, name }) => {
          if (pattern.test(sql)) {
            throw new Error(
              `${templateFile} contains potential SQL injection pattern: ${name}\n` +
              `   Use parameterized queries only.`
            );
          }
          expect(sql).not.toMatch(pattern);
        });
      });

      test(`Must not contain dynamic SQL`, () => {
        const dynamicSqlPatterns = [
          { pattern: /EXEC\s*\(/i, name: "EXEC(" },
          { pattern: /EXECUTE\s*\(/i, name: "EXECUTE(" },
          { pattern: /sp_executesql/i, name: "sp_executesql" },
          { pattern: /EXECUTE\s+IMMEDIATE/i, name: "EXECUTE IMMEDIATE" }
        ];
        
        dynamicSqlPatterns.forEach(({ pattern, name }) => {
          if (pattern.test(sql)) {
            throw new Error(
              `${templateFile} contains dynamic SQL: ${name}\n` +
              `   Dynamic SQL is not allowed for security reasons.`
            );
          }
          expect(sql).not.toMatch(pattern);
        });
      });
    });

    // ==================== ALLOWED ACTIONS (UNCHANGED) ====================
    
    if (policy.rules.allowed_actions && Array.isArray(policy.rules.allowed_actions)) {
      describe("Allowed Actions Policy", () => {
        test(`Must only use allowed SQL actions`, () => {
          const allowedActions = policy.rules.allowed_actions;
          
          const actionPattern = /\b(SELECT|WITH|DELETE|INSERT|UPDATE|DROP|ALTER|CREATE|TRUNCATE)\b/gi;
          const foundActions = [...new Set(
            (sql.match(actionPattern) || []).map(a => a.toUpperCase())
          )];
          
          const violatingActions = foundActions.filter(
            action => !allowedActions.includes(action)
          );
          
          if (violatingActions.length > 0) {
            throw new Error(
              `${templateFile} uses disallowed actions: ${violatingActions.join(', ')}\n` +
              `   Allowed: ${allowedActions.join(', ')}`
            );
          }
          
          expect(foundActions.length).toBeGreaterThan(0);
          expect(violatingActions).toHaveLength(0);
        });
      });
    }

    // ==================== DISALLOWED ACTIONS (UNCHANGED) ====================
    
    if (policy.rules.disallowed_actions && Array.isArray(policy.rules.disallowed_actions)) {
      describe("Disallowed Actions Policy", () => {
        policy.rules.disallowed_actions.forEach(disallowedAction => {
          test(`Must NOT contain ${disallowedAction}`, () => {
            const actionPattern = new RegExp(`\\b${disallowedAction}\\b`, 'gi');
            const sqlWithoutComments = sql
              .replace(/--.*$/gm, '')
              .replace(/\/\*[\s\S]*?\*\//g, '');
            
            const matches = sqlWithoutComments.match(actionPattern);
            
            if (matches) {
              throw new Error(
                `${templateFile} contains disallowed action: ${disallowedAction}\n` +
                `   Found ${matches.length} occurrence(s)`
              );
            }
            
            expect(sqlWithoutComments).not.toMatch(actionPattern);
          });
        });
      });
    }

    // ==================== ROW LIMIT POLICY (UNCHANGED) ====================
    
    if (typeof policy.rules.row_limit === 'number') {
      describe("Row Limit Policy", () => {
        test(`LIMIT clauses must not exceed ${policy.rules.row_limit} rows`, () => {
          const rowLimit = policy.rules.row_limit;
          const violations = [];
          
          // Check LIMIT clauses
          const limitPattern = /LIMIT\s+(\d+)/gi;
          let match;
          while ((match = limitPattern.exec(sql)) !== null) {
            const limit = parseInt(match[1]);
            if (limit > rowLimit) {
              violations.push(`LIMIT ${limit} exceeds policy limit of ${rowLimit}`);
            }
          }
          
          // Check TOP clauses
          const topPattern = /TOP\s+(\d+)/gi;
          while ((match = topPattern.exec(sql)) !== null) {
            const limit = parseInt(match[1]);
            if (limit > rowLimit) {
              violations.push(`TOP ${limit} exceeds policy limit of ${rowLimit}`);
            }
          }
          
          if (violations.length > 0) {
            throw new Error(
              `${templateFile} has row limit violations:\n` +
              violations.map(v => `   - ${v}`).join('\n')
            );
          }
          
          expect(violations).toHaveLength(0);
        });

        test(`Should have LIMIT clause or explicit documentation`, () => {
          const hasAggregation = /\b(COUNT|SUM|AVG|MIN|MAX|GROUP\s+BY)\b/i.test(sql);
          const hasLimit = /\b(LIMIT|TOP)\b/i.test(sql);
          const hasApplicationLimitComment = 
            /--\s*LIMIT handled by application/i.test(sql) ||
            /--\s*Row limit:\s*application-controlled/i.test(sql);
          
          // Pass if aggregation, has limit, or has comment
          expect(hasAggregation || hasLimit || hasApplicationLimitComment).toBe(true);
        });
      });
    }

    // ==================== COMPLIANCE SUMMARY (UPDATED) ====================
    
    describe("Compliance Summary", () => {
      test(`Generate compliance report`, () => {
        if (isExempt) {
          console.log(`   Skipping compliance checks for exempt file: ${templateFile}`);
          return;
        }
        
        const result = hasValidTemporalFilter(sql);
        
        const compliance = {
          template: templateFile,
          policy_version: policyData.version || "unknown",
          checks: {
            temporal_filter_required: {
              has_temporal_filter: result.valid,
              details: result.found,
              validation_rules: {
                column_suffixes: DATE_COLUMN_SUFFIXES,
                window_parameters: WINDOW_PARAMETERS,
                temporal_operators: TEMPORAL_OPERATORS
              }
            },
            security: {
              has_tenant_id: /WHERE[\s\S]*?tenant_id\s*=\s*[@$:]/i.test(sql),
              no_select_star: !/SELECT\s+\*(?!\s*FROM\s*\()/i.test(sql),
              no_joins: !/\bJOIN\b/i.test(sql)
            },
            allowed_actions: {
              status: policy.rules.allowed_actions ? "checked" : "skipped",
              found: policy.rules.allowed_actions ? extractActions(sql, policy.rules.allowed_actions) : []
            },
            disallowed_actions: {
              status: policy.rules.disallowed_actions ? "checked" : "skipped",
              violations: policy.rules.disallowed_actions ? extractActions(sql, policy.rules.disallowed_actions) : []
            },
            row_limit: {
              policy_max: policy.rules.row_limit || "not configured",
              has_limit: /\b(LIMIT|TOP)\b/i.test(sql),
              is_aggregation_query: /\b(COUNT|SUM|AVG|MIN|MAX|GROUP\s+BY)\b/i.test(sql)
            }
          }
        };
        
        console.log(`\nCompliance Report for ${templateFile}:`);
        console.log(JSON.stringify(compliance, null, 2));
        
        // MANDATORY: All templates must have temporal filter
        expect(compliance.checks.temporal_filter_required.has_temporal_filter).toBe(true);
        
        // MANDATORY: All templates must have tenant_id filter
        expect(compliance.checks.security.has_tenant_id).toBe(true);
      });
    });
  });
});

// ==================== AGGREGATE SUMMARY (UPDATED) ====================

describe("Policy Guardrails: Aggregate Summary", () => {
  test("Display overall policy compliance across all templates", () => {
    const summary = {
      total_templates: templateFiles.length,
      configured_policies: Object.keys(policy.rules).filter(key => 
        policy.rules[key] !== undefined && policy.rules[key] !== null
      ),
      templates: templateFiles,
      temporal_filter_validation: {
        method: "SIMPLIFIED - Keyword presence detection",
        column_suffixes: DATE_COLUMN_SUFFIXES,
        window_parameters: WINDOW_PARAMETERS,
        temporal_operators: TEMPORAL_OPERATORS
      },
      policy_snapshot: {
        allowed_actions: policy.rules.allowed_actions || "not configured",
        disallowed_actions: policy.rules.disallowed_actions || "not configured",
        row_limit: policy.rules.row_limit || "not configured",
        max_window_days: policy.rules.max_window_days || "not configured",
        disallow_future_as_of: policy.rules.disallow_future_as_of || "not configured"
      }
    };
    
    console.log("\n" + "=".repeat(60));
    console.log("OVERALL POLICY COMPLIANCE SUMMARY");
    console.log("=".repeat(60));
    console.log(JSON.stringify(summary, null, 2));
    console.log("=".repeat(60) + "\n");
    
    expect(templateFiles.length).toBeGreaterThan(0);
  });
});

// ==================== EXPORTS ====================

module.exports = {
  extractActions,
  hasValidTemporalFilter,
  isDateColumn,
  isDateParameter,
  DATE_COLUMN_SUFFIXES,
  WINDOW_PARAMETERS,
  TEMPORAL_OPERATORS
};