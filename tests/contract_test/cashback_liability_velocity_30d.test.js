const { z } = require("zod");
const fs = require("fs");
const path = require("path");
const { runContractTests } = require("./validators/validate_contract");

const metaPath = path.join(__dirname, "../../intents/cashback_liability_velocity_30d.meta.json");

const schema = z.object({
  customer_id: z.string(),
  liability_30d_ago: z.number(),
  liability_today: z.number(),
  delta_liability: z.number(),
  yyyy_mm: z.number(),
});

runContractTests(metaPath, schema);