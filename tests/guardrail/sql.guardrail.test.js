const fs = require("fs");
const path = require("path");

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
 * Extracts temporal filters from SQL
 * Returns array of objects with: { column, operator, parameters }
 */
function extractTemporalFilters(sql) {
  const filters = [];
  const sqlWithoutComments = sql.replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');
  
  // Build pattern for date columns
  const dateColumnPattern = `(?:CAST\\s*\\(\\s*)?(\\w+)(?:\\s+AS\\s+\\w+\\s*\\))?`;
  
  // Pattern 1: Handle BETWEEN specifically (must match BETWEEN...AND together)
  const betweenPattern = new RegExp(
    `${dateColumnPattern}\\s*BETWEEN\\s+([\\s\\S]*?)\\s+AND\\s+([\\s\\S]*?)(?=\\s*(?:AND\\s+\\w+\\s*[=<>]|OR|GROUP|ORDER|LIMIT|TOP|\\)|;|$))`,
    'gi'
  );
  
  let match;
  while ((match = betweenPattern.exec(sqlWithoutComments)) !== null) {
    const column = match[1];
    const startExpression = match[2];
    const endExpression = match[3];
    
    // Only process if column is date-named
    if (!isDateColumn(column)) {
      continue;
    }
    
    // Extract parameters from both expressions
    const paramPattern = /@(\w+)/g;
    const parameters = [];
    let paramMatch;
    
    const fullExpression = startExpression + ' ' + endExpression;
    while ((paramMatch = paramPattern.exec(fullExpression)) !== null) {
      parameters.push(paramMatch[1]);
    }
    
    // Check if all parameters are date-related
    const allParamsValid = parameters.length > 0 && 
                          parameters.every(p => isDateParameter(p));
    
    if (allParamsValid) {
      filters.push({
        column,
        operator: 'BETWEEN',
        parameters,
        fullMatch: match[0].substring(0, 150)
      });
    }
  }
  
  // Pattern 2: Handle IN operator
  const inPattern = new RegExp(
    `${dateColumnPattern}\\s+IN\\s*\\(([^)]+)\\)`,
    'gi'
  );
  
  while ((match = inPattern.exec(sqlWithoutComments)) !== null) {
    const column = match[1];
    const inExpression = match[2];
    
    if (!isDateColumn(column)) {
      continue;
    }
    
    const paramPattern = /@(\w+)/g;
    const parameters = [];
    let paramMatch;
    while ((paramMatch = paramPattern.exec(inExpression)) !== null) {
      parameters.push(paramMatch[1]);
    }
    
    const allParamsValid = parameters.length > 0 && 
                          parameters.every(p => isDateParameter(p));
    
    if (allParamsValid) {
      filters.push({
        column,
        operator: 'IN',
        parameters,
        fullMatch: match[0].substring(0, 150)
      });
    }
  }
  
  // Pattern 3: Handle comparison operators (=, >=, <=, >, <)
  const comparisonPattern = new RegExp(
    `${dateColumnPattern}\\s*(=|>=|<=|>|<)\\s+([\\s\\S]*?)(?=\\s+(?:AND\\s+\\w+\\s*[=<>]|OR|GROUP|ORDER|LIMIT|TOP|\\)|;|$))`,
    'gi'
  );
  
  while ((match = comparisonPattern.exec(sqlWithoutComments)) !== null) {
    const column = match[1];
    const operator = match[2];
    const valueExpression = match[3];
    
    if (!isDateColumn(column)) {
      continue;
    }
    
    const paramPattern = /@(\w+)/g;
    const parameters = [];
    let paramMatch;
    while ((paramMatch = paramPattern.exec(valueExpression)) !== null) {
      parameters.push(paramMatch[1]);
    }
    
    const allParamsValid = parameters.length > 0 && 
                          parameters.every(p => isDateParameter(p));
    
    if (allParamsValid) {
      filters.push({
        column,
        operator: operator.toUpperCase(),
        parameters,
        fullMatch: match[0].substring(0, 150)
      });
    }
  }
  
  return filters;
}

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
        console.log(`   ⚠️  ${templateFile} is marked as EXEMPT (test file)`);
      }
    });

    // ==================== MANDATORY TEMPORAL FILTER (UPDATED) ====================
    
    describe("Mandatory Temporal Date Filter", () => {
      test(`Must include temporal date filter with proper naming conventions`, () => {
        if (isExempt) {
          console.log(`   ⏭️  Skipping temporal filter check for exempt file: ${templateFile}`);
          return;
        }
        
        const temporalFilters = extractTemporalFilters(sql);
        
        if (temporalFilters.length === 0) {
          throw new Error(
            `${templateFile} is missing required temporal date filter.\n` +
            `   Every query must filter by a date column for performance and data correctness.\n\n` +
            `   Requirements:\n` +
            `   - Column must end with: ${DATE_COLUMN_SUFFIXES.join(', ')}\n` +
            `   - Must use operators: ${TEMPORAL_OPERATORS.join(', ')}\n` +
            `   - Parameters must be date-named (e.g., @as_of_date, @start_date) or window params (e.g., @window_days)\n\n` +
            `   Examples:\n` +
            `   ✓ order_date BETWEEN @start_date AND @end_date\n` +
            `   ✓ as_of_date = @as_of_date\n` +
            `   ✓ as_of_date IN (@date1, @date2)\n` +
            `   ✓ CAST(order_date AS DATE) BETWEEN DATEADD(DAY, -@window_days, @as_of_date) AND @as_of_date\n` +
            `   ✗ random_column = @param (column not date-named)\n` +
            `   ✗ order_date = @customer_id (parameter not date-named)\n` +
            `   ✗ order_date = '2024-01-01' (hardcoded literal, not parameterized)`
          );
        }
        
        // Log found filters for visibility
        if (temporalFilters.length > 1) {
          console.log(`   ℹ️  Found ${temporalFilters.length} temporal filters in ${templateFile}:`);
          temporalFilters.forEach(f => {
            console.log(`      - ${f.column} ${f.operator} using params: ${f.parameters.join(', ')}`);
          });
        }
        
        expect(temporalFilters.length).toBeGreaterThan(0);
      });

      test(`Temporal filter must use parameterized values (no hardcoded dates)`, () => {
        if (isExempt) {
          console.log(`   ⏭️  Skipping hardcoded date check for exempt file: ${templateFile}`);
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
          console.log(`   ⏭️  Skipping parameter naming check for exempt file: ${templateFile}`);
          return;
        }
        
        const temporalFilters = extractTemporalFilters(sql);
        
        // This test is redundant with the first test (which already validates param names)
        // but kept for clarity and explicit validation
        const invalidFilters = temporalFilters.filter(f => 
          !f.parameters.every(p => isDateParameter(p))
        );
        
        if (invalidFilters.length > 0) {
          throw new Error(
            `${templateFile} has temporal filters with invalid parameter names:\n` +
            invalidFilters.map(f => 
              `   - ${f.column} uses: ${f.parameters.join(', ')}`
            ).join('\n') + '\n' +
            `   Parameters must be date-named (e.g., @as_of_date) or window params (e.g., @window_days)`
          );
        }
        
        expect(invalidFilters).toHaveLength(0);
      });
    });

    // ==================== SECURITY REQUIREMENTS (UNCHANGED) ====================
    
    describe("Security Requirements", () => {
      test(`Must include tenant_id filter`, () => {
        if (isExempt) {
          console.log(`   ⏭️  Skipping tenant_id check for exempt file: ${templateFile}`);
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
          console.log(`   ⏭️  Skipping compliance checks for exempt file: ${templateFile}`);
          return;
        }
        
        const temporalFilters = extractTemporalFilters(sql);
        
        const compliance = {
          template: templateFile,
          policy_version: policyData.version || "unknown",
          checks: {
            temporal_filter_required: {
              has_temporal_filter: temporalFilters.length > 0,
              filters_found: temporalFilters.map(f => ({
                column: f.column,
                operator: f.operator,
                parameters: f.parameters
              })),
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

// ==================== HELPER FUNCTIONS ====================

function extractActions(sql, actionList) {
  const found = [];
  const sqlWithoutComments = sql
    .replace(/--.*$/gm, '')
    .replace(/\/\*[\s\S]*?\*\//g, '');
  
  actionList.forEach(action => {
    const pattern = new RegExp(`\\b${action}\\b`, 'gi');
    if (pattern.test(sqlWithoutComments)) {
      found.push(action);
    }
  });
  
  return found;
}

module.exports = {
  extractActions,
  extractTemporalFilters,
  isDateColumn,
  isDateParameter,
  DATE_COLUMN_SUFFIXES,
  WINDOW_PARAMETERS,
  TEMPORAL_OPERATORS
};