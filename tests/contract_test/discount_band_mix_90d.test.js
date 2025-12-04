const { z } = require("zod");
const fs = require("fs");
const path = require("path");
const { runContractTests } = require("./validators/validate_contract");

const metaPath = path.join(__dirname, "../../intents/discount_band_mix_90d.meta.json");

const schema = z.object({
  discount_band: z.number(),
  gmv_per_band: z.number(),
  pct_of_total_gmv: z.number(),
});

runContractTests(metaPath, schema);