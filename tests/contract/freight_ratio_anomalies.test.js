const { z } = require("zod");
const fs = require("fs");
const path = require("path");
const { runContractTests } = require("./validators/validate_contract");

const metaPath = path.join(__dirname, "../../intents/freight_ratio_anomalies.meta.json");

const schema = z.object({
  order_id: z.number(),
  order_date: z.date(),
  net_amount: z.number(),
  freight_amount: z.number(),
  freight_ratio: z.number(),
  yyyy_mm: z.number(),
});

runContractTests(metaPath, schema);