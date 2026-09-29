import { balanceWorker } from "./workers/balance.js";
import {
  executionWorker,
  notificationWorker,
} from "./workers/scheduler.js";
import { transactionConfirmationWorker } from "./workers/transactionConfirmation.js";
import batchPaymentWorker from "./workers/batchPayment.js";
import { exportWorker } from "./workers/export.js";
import { webhookWorker } from "./queues/webhook.js";
import { subClient } from "./config/redis.js";
import UssdService from "./services/UssdService.js";

const SHUTDOWN_TIMEOUT_MS = 30_000;

export const workers = [
  balanceWorker,
  executionWorker,
  notificationWorker,
  transactionConfirmationWorker,
  batchPaymentWorker,
  exportWorker,
  webhookWorker,
].filter(Boolean);

let shuttingDown = false;

const ussdCleanupInterval = setInterval(() => {
  UssdService.cleanupExpiredSessions();
}, 5 * 60 * 1000);

export const shutdownWorkers = async (signal = "SIGTERM") => {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(`🛑 ${signal} received. Shutting down workers...`);

  clearInterval(ussdCleanupInterval);

  const shutdown = async () => {
    await Promise.all(
      workers.map((worker) =>
        worker.pause().catch((error) => {
          console.error(`❌ Failed to pause ${worker.name}:`, error);
        })
      )
    );

    await Promise.all(
      workers.map((worker) =>
        worker.close().catch((error) => {
          console.error(`❌ Failed to close ${worker.name}:`, error);
        })
      )
    );

    if (subClient?.isOpen) {
      await subClient.quit();
    }
  };

  try {
    await Promise.race([
      shutdown(),
      new Promise((_, reject) =>
        setTimeout(
          () => reject(new Error("Worker shutdown timed out")),
          SHUTDOWN_TIMEOUT_MS
        )
      ),
    ]);

    console.log("✅ Workers shut down gracefully");
  } catch (error) {
    console.error("❌ Graceful worker shutdown failed:", error.message);
  } finally {
    process.exit(0);
  }
};

process.once("SIGTERM", () => shutdownWorkers("SIGTERM"));
process.once("SIGINT", () => shutdownWorkers("SIGINT"));
