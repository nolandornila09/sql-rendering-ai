const { z } = require("zod");
const fs = require("fs");
const path = require("path");
const { runContractTests } = require("./validators/validate_contract");

const metaPath = path.join(__dirname, "../../intents/ar_spike_14v60.meta.json");

const schema = z.object({
  customer_id: z.string(),
  overdue_14d: z.number(),
  overdue_prev_60d: z.number(),
  spike_pct: z.number(),
});

runContractTests(metaPath, schema);