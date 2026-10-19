import { Kafka, logLevel } from "kafkajs";
import { brokers, clientId, topic } from "./config";
export const kafka = new Kafka({ clientId, brokers, logLevel: logLevel.ERROR });
export async function ensureTopic() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({ waitForLeaders: true, topics: [{ topic, numPartitions: 3, replicationFactor: 1 }] });
  } finally { await admin.disconnect(); }
}