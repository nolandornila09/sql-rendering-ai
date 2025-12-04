const { z } = require("zod");
const fs = require("fs");
const path = require("path");
const { runContractTests } = require("./validators/validate_contract");

const metaPath = path.join(__dirname, "../../intents/aging_migration_61plus_wow.meta.json");

const schema = z.object({
  customer_id: z.string(),
  week_start: z.string(),
  overdue_61_plus: z.number(),
  delta_wow: z.number(),
});

runContractTests(metaPath, schema);