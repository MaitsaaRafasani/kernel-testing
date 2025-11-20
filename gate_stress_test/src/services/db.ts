import { Pool } from "pg"

export class PsqlClient {
    private pool: Pool;

    constructor() {
        this.pool = new Pool({
            user: process.env.DB_USER || "postgres",
            host: process.env.DB_HOST || "localhost",
            database: process.env.DB_NAME || "postgres",
            password: process.env.DB_PASS || "postgres",
            port: Number(process.env.DB_PORT) || 5432,
            max: Number(process.env.DB_POOL_MAX) || 200,
        });

        this.pool.on("error", (err) => {
            console.error("Unexpected error on idle client", err);
        });
    }

    async query(text: string, params?: any[]) {
        const client = await this.pool.connect();
        try {
            const result = await client.query(text, params);
            return result.rows;
        } finally {
            client.release();
        }
    }
}

export const db = new PsqlClient();
