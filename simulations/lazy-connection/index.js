import { Client } from "pg";

const CONCURRENT_RUNS = parseInt(process.env.CONCURRENT_RUNS) | 10;
const DURATION_MS = (parseInt(process.env.DURATION_SECONDS) || 30) * 1000;

async function scanCustomersWithOwnConnection(runId) {
  const startedAt = Date.now();
  console.log(`[run ${runId}] connecting`);

  const client = new Client({
    host: process.env.PGHOST || "localhost",
    port: process.env.PGPORT || 5432,
    user: process.env.PGUSER || "postgres",
    password: process.env.PGPASSWORD || "postgres",
    database: process.env.PGDATABASE || "postgres",
  });

  try {
    await client.connect();
    console.log(`[run ${runId}] connected (${Date.now() - startedAt}ms)`);

    const holdMs = 5000 + Math.floor(Math.random() * 5000);
    console.log(`[run ${runId}] holding connection idle for ${holdMs}ms`);
    await new Promise((resolve) => setTimeout(resolve, holdMs));

    console.log(`[run ${runId}] done: ${Date.now() - startedAt}ms`);
  } catch (err) {
    console.error(`[run ${runId}] failed after ${Date.now() - startedAt}ms:`, err.message);
    throw err;
  } finally {
    await client.end();
  }
}

async function main() {
  console.log(
    `Running for ${DURATION_MS}ms, spawning ${CONCURRENT_RUNS} concurrent connections per batch`
  );
  const startedAt = Date.now();

  let batchNumber = 0;
  let totalSucceeded = 0;
  let totalFailed = 0;
  let lastFailureReason;
  let runId = 0;

  while (Date.now() - startedAt < DURATION_MS) {
    batchNumber += 1;
    console.log(`\n-- batch ${batchNumber} --`);

    const runs = Array.from({ length: CONCURRENT_RUNS }, () =>
      scanCustomersWithOwnConnection(++runId)
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
}

main();
