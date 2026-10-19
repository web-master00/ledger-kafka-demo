import "dotenv/config";
export const port = Number(process.env.PORT ?? 3000);
export const databaseUrl = process.env.DATABASE_URL ?? "postgres://demo:demo@localhost:5432/ledger_demo";
export const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",").map(x => x.trim()).filter(Boolean);
export const clientId = process.env.KAFKA_CLIENT_ID ?? "ledger-demo";
export const topic = "ledger.transactions";
export const pollMs = Number(process.env.OUTBOX_POLL_MS ?? 1000);