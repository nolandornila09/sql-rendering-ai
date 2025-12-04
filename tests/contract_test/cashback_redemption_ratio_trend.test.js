const { z } = require("zod");
const fs = require("fs");
const path = require("path");
const { runContractTests } = require("./validators/validate_contract");

const metaPath = path.join(__dirname, "../../intents/cashback_redemption_ratio_trend.meta.json");

const schema = z.object({
  year_no: z.number(),
  week_no: z.number(),
  yyyy_mm: z.date(),
  accrued: z.number(),
  redeemed: z.number(),
  redemption_ratio: z.number(),
});

runContractTests(metaPath, schema);