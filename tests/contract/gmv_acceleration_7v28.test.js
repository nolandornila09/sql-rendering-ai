const { z } = require("zod");
const fs = require("fs");
const path = require("path");
const { runContractTests } = require("./validators/validate_contract");

const metaPath = path.join(__dirname, "../../intents/gmv_acceleration_7v28.meta.json");

const schema = z.object({
  customer_id: z.string(),
  gmv_7d: z.number(),
  gmv_28d: z.number(),
  accel_pct: z.number(),
});

runContractTests(metaPath, schema);