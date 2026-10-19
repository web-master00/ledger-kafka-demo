import { pool } from "./db";
import { ensureTopic, kafka } from "./kafka";
import { pollMs, topic } from "./config";

// Single-process demo publisher. FOR UPDATE SKIP LOCKED supports competing workers,
// but production code should claim rows with a lease/status to avoid holding DB locks
// across network calls. This sample keeps the flow readable for a live walkthrough.
async function main() {
  await ensureTopic();
  const producer = kafka.producer();
  await producer.connect();
  console.log(`Outbox publisher sending to ${topic}`);
  let stopping = false;
  process.on("SIGINT", () => { stopping = true; });
  process.on("SIGTERM", () => { stopping = true; });
  while (!stopping) {
    const client = await pool.connect();
    try {
      const result = await client.query(
        `SELECT id, aggregate_id, event_type, payload
         FROM outbox_events WHERE published_at IS NULL
         ORDER BY created_at ASC LIMIT 50`
      );
      for (const row of result.rows) {
        try {
          await producer.send({ topic, acks: -1, messages: [{ key: row.aggregate_id, value: JSON.stringify(row.payload), headers: { eventType: row.event_type, eventId: row.id } }] });
          await client.query("UPDATE outbox_events SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = $1", [row.id]);
          console.log(JSON.stringify({ message: "Published outbox event", eventId: row.id, transactionId: row.aggregate_id }));
        } catch (error) {
          await client.query("UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1", [row.id, String(error)]);
          console.error("Publish failed; will retry", row.id, error);
        }
      }
    } catch (error) { console.error("Outbox poll failed", error); }
    finally { client.release(); }
    await new Promise(resolve => setTimeout(resolve, pollMs));
  }
  await producer.disconnect(); await pool.end(); process.exit(0);
}
main().catch(err => { console.error(err); process.exit(1); });