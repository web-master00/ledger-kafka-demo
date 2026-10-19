import { Pool } from "pg";
import { databaseUrl } from "./config";
export const pool = new Pool({ connectionString: databaseUrl });