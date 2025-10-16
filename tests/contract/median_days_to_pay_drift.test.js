const { z } = require("zod");
const fs = require("fs");
const path = require("path");
const { runContractTests } = require("./validators/validate_contract");

const metaPath = path.join(__dirname, "../../intents/median_days_to_pay_drift.meta.json");

const schema = z.object({
  customer_id: z.string(),
  delta_days: z.number(),
});

runContractTests(metaPath, schema);