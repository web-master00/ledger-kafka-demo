import express from "express";
import { randomUUID } from "node:crypto";
import { pool } from "./db";
import { port } from "./config";
import { transactionInput, type TransactionAcceptedEvent } from "./events";

async function main() {
  const app = express();
  app.use(express.json());
  app.get("/health", async (_req, res) => {
    try { await pool.query("SELECT 1"); res.json({ status: "ok" }); }
    catch { res.status(503).json({ status: "database_unavailable" }); }
  });

  app.post("/transactions", async (req, res) => {
    const parsed = transactionInput.safeParse(req.body);
    if (!parsed.success) return res.status(400).json({ error: "Invalid payload", details: parsed.error.flatten() });
    const input = parsed.data;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const existing = await client.query(
        "SELECT id, status FROM transactions WHERE idempotency_key = $1 FOR UPDATE", [input.idempotencyKey]
      );
      if (existing.rowCount) {
        await client.query("COMMIT");
        return res.status(200).json({ transactionId: existing.rows[0].id, status: existing.rows[0].status, idempotentReplay: true });
      }
      const transactionId = randomUUID();
      const eventId = randomUUID();
      const event: TransactionAcceptedEvent = {
        eventId, eventType: "TransactionAccepted", occurredAt: new Date().toISOString(),
        transactionId, accountId: input.accountId, amountMinor: input.amountMinor, currency: input.currency
      };
      await client.query(
        `INSERT INTO transactions (id, idempotency_key, account_id, amount_minor, currency)
         VALUES ($1, $2, $3, $4, $5)`,
        [transactionId, input.idempotencyKey, input.accountId, input.amountMinor, input.currency]
      );
      await client.query(
        `INSERT INTO outbox_events (id, aggregate_id, event_type, payload)
         VALUES ($1, $2, $3, $4::jsonb)`,
        [eventId, transactionId, event.eventType, JSON.stringify(event)]
      );
      await client.query("COMMIT");
      console.log(JSON.stringify({ level: "info", message: "Transaction and outbox event committed", transactionId, eventId }));
      return res.status(202).json({ transactionId, eventId, status: "accepted", note: "Accepted for asynchronous processing" });
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      // Concurrent requests with the same key can race; a production API should handle
      // the unique-constraint conflict by loading and returning the existing transaction.
      console.error("Transaction request failed", error);
      return res.status(500).json({ error: "Could not accept transaction" });
    } finally { client.release(); }
  });

  const server = app.listen(port, () => console.log(`Transaction API listening at http://localhost:${port}`));
  const shutdown = async () => { server.close(); await pool.end(); process.exit(0); };
  process.on("SIGINT", shutdown); process.on("SIGTERM", shutdown);
}
main().catch(err => { console.error(err); process.exit(1); });