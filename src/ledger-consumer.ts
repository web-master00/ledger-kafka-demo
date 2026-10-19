import { pool } from "./db";
import { ensureTopic, kafka } from "./kafka";
import { topic } from "./config";
import type { TransactionAcceptedEvent } from "./events";

async function main() {
  await ensureTopic();
  const consumer = kafka.consumer({ groupId: "ledger-writer-v1" });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  console.log(`Ledger consumer listening to ${topic}`);

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic: messageTopic, partition, message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString()) as TransactionAcceptedEvent;
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const seen = await client.query("SELECT event_id FROM processed_events WHERE event_id = $1", [event.eventId]);
        if (seen.rowCount === 0) {
          await client.query(
            `INSERT INTO ledger_entries (transaction_id, account_id, amount_minor, currency)
             VALUES ($1, $2, $3, $4) ON CONFLICT (transaction_id) DO NOTHING`,
            [event.transactionId, event.accountId, event.amountMinor, event.currency]
          );
          await client.query("INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING", [event.eventId]);
          await client.query("UPDATE transactions SET status = 'processed' WHERE id = $1", [event.transactionId]);
          console.log(JSON.stringify({ service: "ledger-consumer", action: "ledger-entry-written", eventId: event.eventId, transactionId: event.transactionId, amountMinor: event.amountMinor, currency: event.currency, partition, offset: message.offset }));
        } else {
          console.log(JSON.stringify({ service: "ledger-consumer", action: "duplicate-ignored", eventId: event.eventId, transactionId: event.transactionId }));
        }
        await client.query("COMMIT");
        await consumer.commitOffsets([{ topic: messageTopic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
      } catch (error) {
        await client.query("ROLLBACK").catch(() => undefined);
        console.error("Processing failed; offset not committed", event.eventId, error);
        throw error;
      } finally { client.release(); }
    }
  });
}
main().catch(err => { console.error("Consumer stopped", err); process.exit(1); });