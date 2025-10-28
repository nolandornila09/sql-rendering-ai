const { z } = require("zod");
const fs = require("fs");

function runContractTests(metaPath, schema) {
  const meta = JSON.parse(fs.readFileSync(metaPath, "utf-8"));

  describe(`${meta.template_id} - Contract Tests`, () => {
    test("validates meta.json structure", () => {
      // Check required filters exist
      expect(meta.required_filters).toBeDefined();
      expect(Array.isArray(meta.required_filters)).toBe(true);
      expect(meta.required_filters.length).toBeGreaterThan(0);
      
      // Check projected columns exist and are not empty
      expect(meta.projected_columns).toBeDefined();
      expect(Array.isArray(meta.projected_columns)).toBe(true);
      expect(meta.projected_columns.length).toBeGreaterThan(0);
      
      // Check partitions exist
      expect(meta.partitions).toBeDefined();
      expect(Array.isArray(meta.partitions)).toBe(true);
      
      // Check defaults exist
      expect(meta.defaults).toBeDefined();
      expect(typeof meta.defaults).toBe("object");
      
      // Check param_formats exist
      expect(meta.param_formats).toBeDefined();
      expect(typeof meta.param_formats).toBe("object");
    });

    test("enforces tenant scoping (RLS)", () => {
      // Validate that tenant_id is in required filters (Row Level Security)
      expect(meta.required_filters).toContain("tenant_id");
    });

    test("validates required filters", () => {
      // tenant_id is always required (checked in RLS test)
      expect(meta.required_filters).toContain("tenant_id");
      
      // At least one date filter must be present (as_of_date, from_date, to_date, etc.)
      const dateFilters = meta.required_filters.filter(filter => 
        filter.includes("date")
      );
      expect(dateFilters.length).toBeGreaterThan(0);
      
      // All date filters should have param_formats
      dateFilters.forEach(filter => {
        expect(meta.param_formats).toHaveProperty(filter);
        expect(meta.param_formats[filter]).toMatch(/date/i);
      });
    });

    test("validates projected columns match schema", () => {
      const schemaKeys = Object.keys(schema.shape);
      const projectedColumns = meta.projected_columns;
      
      // All projected columns should have schema definitions
      projectedColumns.forEach(column => {
        expect(schemaKeys).toContain(column);
      });
      
      // All schema keys should be in projected columns
      schemaKeys.forEach(key => {
        expect(projectedColumns).toContain(key);
      });
    });

    test("validates partitions structure", () => {
      // Check that partitions are properly defined
      expect(meta.partitions).toContain("yyyy_mm");
      
      // Validate partition format
      meta.partitions.forEach(partition => {
        expect(typeof partition).toBe("string");
        expect(partition.length).toBeGreaterThan(0);
      });
    });

    test("validates template_id is properly set", () => {
      expect(meta.template_id).toBeDefined();
      expect(meta.template_id.length).toBeGreaterThan(0);
      expect(typeof meta.template_id).toBe("string");
      // Ensure 'name' is not used instead of 'template_id'
      expect(meta.name).toBeUndefined();
    });

    test("validates defaults object exists and has valid entries", () => {
      expect(meta.defaults).toBeDefined();
      expect(typeof meta.defaults).toBe("object");
      expect(Object.keys(meta.defaults).length).toBeGreaterThan(0);
      
      // All defaults should have corresponding param_formats
      Object.keys(meta.defaults).forEach(key => {
        expect(meta.param_formats).toHaveProperty(key);
      });
    });

    test("validates param_formats object and completeness", () => {
      expect(meta.param_formats).toBeDefined();
      expect(typeof meta.param_formats).toBe("object");
      expect(Object.keys(meta.param_formats).length).toBeGreaterThan(0);
      
      // All param_formats should have format strings
      Object.entries(meta.param_formats).forEach(([key, format]) => {
        expect(typeof format).toBe("string");
        expect(format.length).toBeGreaterThan(0);
      });
    });

    test("validates all required filters have param_formats", () => {
      meta.required_filters.forEach(filter => {
        expect(meta.param_formats).toHaveProperty(filter);
      });
    });

    test("validates all defaults have param_formats", () => {
      Object.keys(meta.defaults).forEach(defaultKey => {
        expect(meta.param_formats).toHaveProperty(defaultKey);
      });
    });

    test("validates default value types match param_formats", () => {
      Object.entries(meta.defaults).forEach(([key, value]) => {
        const format = meta.param_formats[key];
        expect(format).toBeDefined();
        
        // Check decimal before integer to handle "decimal,integer" formats
        if (format.includes("decimal")) {
          expect(typeof value).toBe("number");
          // Reject NaN and Infinity
          expect(isNaN(value)).toBe(false);
          expect(isFinite(value)).toBe(true);
          // Non-negative check for "pct" or "percentage" params
          if (key.includes("pct") || key.includes("percentage")) {
            expect(value).toBeGreaterThanOrEqual(0);
          }
        } else if (format.includes("integer")) {
          expect(typeof value).toBe("number");
          expect(Number.isInteger(value)).toBe(true);
          expect(isNaN(value)).toBe(false);
          // Positive check for limit/count params
          if (key.includes("limit") || key.includes("count")) {
            expect(value).toBeGreaterThan(0);
          }
          // Positive check for window/period params
          if (key.includes("win") || key.includes("weeks") || key.includes("days")) {
            expect(value).toBeGreaterThan(0);
          }
          // Allow delta/offset to be negative
          if (!key.includes("delta") && !key.includes("offset")) {
            expect(value).toBeGreaterThanOrEqual(0);
          }
        } else if (format.includes("string")) {
          expect(typeof value).toBe("string");
          // String should not be empty
          expect(value.length).toBeGreaterThan(0);
          // Enum-like strings should be uppercase or match convention
          if (key.includes("direction") || key.includes("sort") || key.includes("order")) {
            expect(value).toMatch(/^[A-Z]+$/);
          }
        } else if (format.includes("date")) {
          expect(typeof value).toBe("string");
          if (format.includes("YYYY-MM-DD")) {
            expect(value).toMatch(/^\d{4}-\d{2}-\d{2}$/);
            // Validate it's an actual date
            const date = new Date(value);
            expect(date.toString()).not.toBe("Invalid Date");
          }
        } else if (format.includes("boolean")) {
          expect(typeof value).toBe("boolean");
        }
      });
    });

    test("validates param_formats have recognizable types", () => {
      const validTypes = [
        "string",
        "integer",
        "decimal",
        "boolean",
        "date",
        "array",
        "object"
      ];
      
      Object.entries(meta.param_formats).forEach(([key, format]) => {
        const hasValidType = validTypes.some(type => 
          format.toLowerCase().includes(type)
        );
        expect(hasValidType).toBe(true);
      });
    });

    test("validates date format specifications", () => {
      Object.entries(meta.param_formats).forEach(([key, format]) => {
        if (format.includes("date")) {
          expect(format).toMatch(/\(.*\)/);
        }
      });
    });

    test("validates no orphaned param_formats", () => {
      const allowedKeys = new Set([
        ...meta.required_filters,
        ...Object.keys(meta.defaults),
        ...meta.projected_columns
      ]);
      
      Object.keys(meta.param_formats).forEach(key => {
        expect(allowedKeys.has(key)).toBe(true);
      });
    });

    test("validates required_filters are subset of param_formats", () => {
      const paramFormatKeys = Object.keys(meta.param_formats);
      
      meta.required_filters.forEach(filter => {
        expect(paramFormatKeys).toContain(filter);
      });
    });

    test("validates partitions contain meaningful values", () => {
      expect(meta.partitions.length).toBeGreaterThan(0);
      
      meta.partitions.forEach(partition => {
        expect(partition.length).toBeGreaterThan(0);
        expect(/^[a-z_]+$/.test(partition)).toBe(true);
      });
    });

    test("validates no duplicate entries in arrays", () => {
      const filtersSet = new Set(meta.required_filters);
      expect(filtersSet.size).toBe(meta.required_filters.length);
      
      const colsSet = new Set(meta.projected_columns);
      expect(colsSet.size).toBe(meta.projected_columns.length);
      
      const partSet = new Set(meta.partitions);
      expect(partSet.size).toBe(meta.partitions.length);
    });

    test("validates consistency between required_filters and projected_columns", () => {
      meta.required_filters.forEach(filter => {
        expect(meta.projected_columns).not.toContain(filter);
      });
    });

    test("validates template_id naming convention", () => {
      expect(meta.template_id).toMatch(/^[a-z]+[a-z0-9_]*$/);
    });
  });
}

module.exports = { runContractTests };