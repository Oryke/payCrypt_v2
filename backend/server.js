```js
import dotenv from "dotenv";
dotenv.config();

import http from "http";
import { validateEnv } from "./config/env.validation.js";
import { validateStartup } from "./services/deploymentValidator.js";
import { syncRenamedMigrations } from "./utils/migrationIds.js";
import stellarStreamService from "./services/StellarStreamService.js";

let validatedEnv;
try {
  validatedEnv = validateEnv(process.env);
  Object.assign(process.env, validatedEnv);
} catch (error) {
  console.error(error.message);
  process.exit(1);
}

const [
  { default: app },
  { default: db, ensureConnectionWithRetry },
  { default: redis },
  ,
  { shutdownWorkers },
  { default: HousekeepingService },
  { default: SocketService },
  { initApollo },
  { tokenPriceRefresh, ngnRateRefresh },
] = await Promise.all([
  import("./app.js"),
  import("./config/database.js"),
  import("./config/redis.js"),
  import("./listeners.js"),
  import("./workers.js"),
  import("./services/HousekeepingService.js"),
  import("./services/SocketService.js"),
  import("./graphql/apollo.js"),
  import("./config/initials.js"),
]);

const PORT = process.env.PORT || 3000;
const TWENTY_FOUR_HOURS = 24 * 60 * 60 * 1000;
const FIVE_MINUTES = 5 * 60 * 1000;

const isProduction = process.env.NODE_ENV === "production";

(async () => {
  const connectionResult = await ensureConnectionWithRetry();

  if (!connectionResult.ok) {
    console.error("Database connection failed after retries:", connectionResult.error);

    if (isProduction) {
      console.error("Exiting: database connection is required in production");
      process.exit(1);
    } else {
      console.warn("Continuing without database in development mode");
    }
  } else {
    try {
      // Rewrite renamed migration records before Knex validates the directory.
      await syncRenamedMigrations(db);
      console.log("Checking pending migrations...");

      const [completed, pending] = await db.migrate.list();

      if (pending.length > 0) {
        console.log("Pending migrations:", pending);

        if (isProduction) {
          console.error("Exiting: pending migrations must be applied before startup in production");
          process.exit(1);
        }
      }

      console.log("Running database migrations...");
      const [batchNo, migrations] = await db.migrate.latest();

      if (migrations.length > 0) {
        console.log("Applied migrations:", migrations.join(", "));
      } else {
        console.log("No new migrations to run");
      }
    } catch (err) {
      console.error("Database migrations failed:", err.message);

      if (isProduction) {
        console.error("Exiting due to migration failure in production");
        process.exit(1);
      } else {
        console.warn("Continuing without migrations in development mode");
      }
    }
  }

  // Start only after migrations complete, so the stream never races the
  // stellar account/tag tables at boot. It reconnects internally on Horizon
  // outages and restores each account's Redis cursor after a restart.
  if (connectionResult.ok) {
    try {
      await stellarStreamService.start();
    } catch (error) {
      console.error("Stellar payment stream failed to start:", error.message);
      if (isProduction) process.exit(1);
    }
  }

  const httpServer = http.createServer(app);

  SocketService.init(httpServer);

  await initApollo(app, null, httpServer);

  // Track background timers for clean shutdown
  const activeTimers = [];

  const SHUTDOWN_DEADLINE_MS = parseInt(
    process.env.SHUTDOWN_DEADLINE_MS || "15000",
    10
  );
  let shuttingDown = false;

  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;

    console.log(
      `${signal} received — starting graceful shutdown (deadline: ${SHUTDOWN_DEADLINE_MS}ms)`
    );

    // Hard deadline: force exit if graceful shutdown takes too long
    const deadline = setTimeout(() => {
      console.error("Graceful shutdown deadline exceeded — forcing exit");
      process.exit(1);
    }, SHUTDOWN_DEADLINE_MS);
    deadline.unref();

    try {
      // 1. Stop accepting new HTTP connections
      await new Promise((resolve) => httpServer.close(resolve));
      console.log("  [1/6] HTTP server closed");

      // 2. Close Socket.IO (disconnect all clients)
      if (SocketService.io) {
        await new Promise((resolve) => SocketService.io.close(resolve));
      }
      console.log("  [2/6] Socket.IO closed");

      // 3. Stop Stellar payment streams
      stellarStreamService.stop();
      console.log("  [3/6] Stellar streams stopped");

      // 4. Stop worker intake, drain active jobs, and close queues
      await shutdownWorkers(Math.max(1000, SHUTDOWN_DEADLINE_MS - 1000));
      console.log("  [4/6] BullMQ workers drained and queues closed");

      // 5. Close Redis connections
      try {
        if (redis.isOpen) await redis.quit();
      } catch {
        /* ignore */
      }
      console.log("  [5/6] Redis closed");

      // 6. Destroy database pool
      try {
        await db.destroy();
      } catch {
        /* ignore */
      }
      console.log("  [6/6] Database pool destroyed");

      // Clear background timers (audit cleanup, export cleanup, USSD)
      for (const id of activeTimers) clearInterval(id);
      tokenPriceRefresh.stop();
      ngnRateRefresh.stop();

      clearTimeout(deadline);

      console.log("  Graceful shutdown complete");

      process.exit(0);
    } catch (err) {
      clearTimeout(deadline);
      console.error("Error during graceful shutdown:", err);
      process.exit(1);
    }
  };

  process.once("SIGTERM", () => shutdown("SIGTERM"));
  process.once("SIGINT", () => shutdown("SIGINT"));

  // Validate deployments before listening. In production, fail startup on missing deployments.
  try {
    const deployResult = await validateStartup({ failOnMissing: isProduction });

    if (!deployResult.ok) {
      console.warn(
        "One or more deployments are missing or mismatched:",
        deployResult.missing.map((m) => m.chain)
      );

      if (isProduction) {
        console.error("Exiting: missing deployments in production environment");
        process.exit(1);
      }
    }
  } catch (err) {
    console.error("Deployment validation failed:", err.message);

    if (isProduction) process.exit(1);
  }

  httpServer.listen(PORT, () => {
    console.log(`Server running on port ${PORT} (with WebSockets)`);
    console.log(`Bull Board: http://localhost:${PORT}/admin/running-queues`);

    const retentionDays =
      parseInt(process.env.AUDIT_LOG_RETENTION_DAYS) || 90;

    // Housekeeping jobs run on every replica's timer, but HousekeepingService
    // wraps each run in a distributed lease (see backend/services/
    // HousekeepingService.js) so only one replica actually executes the work
    // per tick; the rest observe the lease held and skip. See
    // backend/docs/housekeeping-jobs.md for details.
    activeTimers.push(
      setInterval(async () => {
        try {
          await HousekeepingService.runAuditLogCleanup(retentionDays);
        } catch (err) {
          console.error("Audit log cleanup failed:", err.message);
        }
      }, TWENTY_FOUR_HOURS)
    );

    console.log(
      `Audit log retention: ${retentionDays} days (cleanup every 24h)`
    );

    // Self-scheduling: the next refresh starts only after the previous one
    // settles, so slow upstream calls never overlap (see config/initials.js).
    tokenPriceRefresh.start();
    ngnRateRefresh.start();

    activeTimers.push(
      setInterval(async () => {
        try {
          await HousekeepingService.runExportCleanup();
        } catch (err) {
          console.error("Export cleanup failed:", err.message);
        }
      }, TWENTY_FOUR_HOURS)
    );

    activeTimers.push(
      setInterval(async () => {
        try {
          await HousekeepingService.runBillPaymentReconciliation();
        } catch (err) {
          console.error("Bill payment reconciliation failed:", err.message);
        }
      }, FIVE_MINUTES)
    );
  });
})();
