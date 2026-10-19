import { z } from "zod";
export const transactionInput = z.object({
  idempotencyKey: z.string().min(8).max(128),
  accountId: z.string().min(1).max(100),
  amountMinor: z.number().int().positive(),
  currency: z.string().regex(/^[A-Za-z]{3}$/).transform(v => v.toUpperCase())
});
export type TransactionInput = z.infer<typeof transactionInput>;
export type TransactionAcceptedEvent = {
  eventId: string; eventType: "TransactionAccepted"; occurredAt: string;
  transactionId: string; accountId: string; amountMinor: number; currency: string;
};