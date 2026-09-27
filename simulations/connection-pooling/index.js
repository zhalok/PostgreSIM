import { Pool } from "pg";
import { createInterface } from "node:readline/promises";

async function promptForParams() {
  const rl = createInterface({ input: process.stdin, output: process.stdout });

  const concurrentAnswer = await rl.question("Concurrency [10]: ");
  const durationAnswer = await rl.question("Duration in seconds [30]: ");
  const poolSizeAnswer = await rl.question("Pool max size [10]: ");

  rl.close();

  const concurrentRuns = parseInt(concurrentAnswer) || 10;
  const durationSeconds = parseInt(durationAnswer) || 30;
  const poolSize = parseInt(poolSizeAnswer) || 10;

  return { concurrentRuns, durationMs: durationSeconds * 1000, poolSize };
}

async function scanCustomersWithPooledConnection(pool, runId) {
  const startedAt = Date.now();
  console.log(`[run ${runId}] acquiring connection from pool`);

  const client = await pool.connect();

  try {
    console.log(`[run ${runId}] acquired (${Date.now() - startedAt}ms)`);

    const holdMs = 5000 + Math.floor(Math.random() * 5000);
    console.log(`[run ${runId}] holding connection idle for ${holdMs}ms`);
    await new Promise((resolve) => setTimeout(resolve, holdMs));

    console.log(`[run ${runId}] done: ${Date.now() - startedAt}ms`);
  } catch (err) {
    console.error(`[run ${runId}] failed after ${Date.now() - startedAt}ms:`, err.message);
    throw err;
  } finally {
    client.release();
  }
}

async function main() {
  const { concurrentRuns, durationMs, poolSize } = await promptForParams();

  const pool = new Pool({
    host: process.env.PGHOST || "localhost",
    port: process.env.PGPORT || 5432,
    user: process.env.PGUSER || "postgres",
    password: process.env.PGPASSWORD || "postgres",
    database: process.env.PGDATABASE || "postgres",
    max: poolSize,
  });

  console.log(
    `\nRunning for ${durationMs}ms, spawning ${concurrentRuns} concurrent connections per batch (pool max ${poolSize})`
  );
  const startedAt = Date.now();

  let batchNumber = 0;
  let totalSucceeded = 0;
  let totalFailed = 0;
  let lastFailureReason;
  let runId = 0;

  while (Date.now() - startedAt < durationMs) {
    batchNumber += 1;
    console.log(`\n-- batch ${batchNumber} --`);

    const runs = Array.from({ length: concurrentRuns }, () =>
      scanCustomersWithPooledConnection(pool, ++runId)
    );

    const results = await Promise.allSettled(runs);

    const succeeded = results.filter((r) => r.status === "fulfilled").length;
    const failed = results.filter((r) => r.status === "rejected");

    totalSucceeded += succeeded;
    totalFailed += failed.length;
    if (failed.length > 0) {
      lastFailureReason = failed[0].reason?.message ?? failed[0].reason;
    }
  }

  console.log(`\nFinished in ${Date.now() - startedAt}ms across ${batchNumber} batches`);
  console.log(`Succeeded: ${totalSucceeded}`);
  console.log(`Failed: ${totalFailed}`);

  if (lastFailureReason) {
    console.log("Sample failure reason:", lastFailureReason);
  }

  await pool.end();
}

main();
