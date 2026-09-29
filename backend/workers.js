import { balanceWorker } from "./workers/balance.js";
import {
  executionWorker,
  notificationWorker,
  schedulerQueue,
  notifierQueue,
} from "./workers/scheduler.js";
import { transactionConfirmationWorker } from "./workers/transactionConfirmation.js";
import batchPaymentWorker from "./workers/batchPayment.js";
import { exportQueue, exportWorker } from "./queues/exportQueue.js";
import UssdService from "./services/UssdService.js";

const workers = [
  balanceWorker,
  executionWorker,
  notificationWorker,
  transactionConfirmationWorker,
  batchPaymentWorker,
  exportWorker,
].filter(Boolean);

const queues = [
  schedulerQueue,
  notifierQueue,
  exportQueue,
].filter(Boolean);

export const shutdownWorkers = async (timeoutMs = 10000) => {
  if (!workers.length && !queues.length) return;

  const timeout = new Promise((_, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Worker shutdown exceeded ${timeoutMs}ms`)),
      timeoutMs
    );
    timer.unref();
  });

  await Promise.race([
    (async () => {
      await Promise.all(workers.map((worker) => worker.pause(true)));
      await Promise.all(workers.map((worker) => worker.close()));
      await Promise.all(queues.map((queue) => queue.close()));
    })(),
    timeout,
  ]);
};

// Clean up expired USSD sessions every 5 minutes
setInterval(() => {
  UssdService.cleanupExpiredSessions();
}, 5 * 60 * 1000);
