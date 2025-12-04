const { z } = require("zod");
const fs = require("fs");
const path = require("path");
const { runContractTests } = require("./validators/validate_contract");

const metaPath = path.join(__dirname, "../../intents/invoices_due_soon_low_paid.meta.json");

const schema = z.object({
  invoice_id: z.number(),
  customer_id: z.string(),
  invoice_date: z.date(),
  due_date: z.date(),
  amount: z.number(),
  paid_to_date: z.date(),
  paid_ratio: z.number(),
  unpaid_ratio: z.number(),
  last_payment_date: z.date(),
  status: z.string(),
});

runContractTests(metaPath, schema);