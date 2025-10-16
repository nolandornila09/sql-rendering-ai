const { z } = require("zod");
const fs = require("fs");
const path = require("path");
const { runContractTests } = require("./validators/validate_contract");

const metaPath = path.join(__dirname, "../../intents/order_outliers_zscore.meta.json");

const schema = z.object({
  order_id: z.number(),
  order_date: z.date(),
  yyyy_mm: z.number(),
  net_amount: z.number(),
  zscore: z.number(),
});

runContractTests(metaPath, schema);