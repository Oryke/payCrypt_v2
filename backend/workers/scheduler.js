```js
import os from "os";
import { randomUUID } from "crypto";
import { Worker, Queue } from "bullmq";
import { redisConnection } from "../config/redis.js";
import ScheduledPayment, { idempotencyKeyFor } from "../models/ScheduledPayment.js";
import Notification from "../models/Notification.js";
import AuditLog from "../models/AuditLog.js";
import PaymentService from "../services/PaymentService.js";
import KeyVaultService from "../services/KeyVaultService.js";
import NotificationService from "../services/NotificationService.js";
import { sendEmail } from "../services/external/smtp.js";
import { apiKeyRotationQueue } from "./apiKeyRotationWorker.js";
import { reconciliationQueue, registerReconciliationJob } from "./reconciliation.js";
import attachRedisErrorAlert from "../utils/bullmqAlerts.js";

// ========== Queues ==========

export const schedulerQueue = redisConnection
    ? new Queue("scheduled-payment-executor", { connection: redisConnection })
    : null;
attachRedisErrorAlert(schedulerQueue, "scheduled-payment-executor-queue");

export const notifierQueue = redisConnection
    ? new Queue("scheduled-payment-notifier", { connection: redisConnection })
    : null;
attachRedisErrorAlert(notifierQueue, "scheduled-payment-notifier-queue");

// ========== Execution Worker ==========
// Runs every 60 seconds — claims due payments, executes them via PaymentService

const WORKER_ID = `${os.hostname()}:${process.pid}:${randomUUID()}`;

export const executionWorker = redisConnection
    ? new Worker(
        "scheduled-payment-executor",
        async (job) => {
            console.log(`⏰ Scheduler: checking for due payments...`);

            // Atomically claim due rows so competing replicas never execute the
            // same payment; expired claims from crashed workers are recovered.
            const duePayments = await ScheduledPayment.claimDuePayments({ workerId: WORKER_ID });

            if (duePayments.length === 0) {
                console.log(`⏰ Scheduler: no due payments found.`);
                return { processed: 0 };
            }

            console.log(
                `⏰ Scheduler: found ${duePayments.length} due payment(s).`
            );
            let processed = 0;
            let failed = 0;

            for (const payment of duePayments) {
                let auditLogId = null;
                try {
                    // Retrieve sender's signing key from vault (encrypted, in-memory only)
                    await KeyVaultService.withUserSecrets(payment.user_id, async (secrets) => {
                        // Log key access for audit trail
                        const auditLog = await AuditLog.create({
                            userId: payment.user_id,
                            action: "key_accessed",
                            resource: "scheduled_payment",
                            resourceId: payment.id,
                            details: { payment_id: payment.id, amount: payment.amount },
                            method: "SCHEDULER",
                            endpoint: "/scheduler/execute-payment",
                        });
                        auditLogId = auditLog.id;
                        console.log(`🔐 Vault: key accessed for payment #${payment.id} (audit log: ${auditLogId})`);

                        // Use the first secret (primary signing key)
                        const signingKey = secrets[0];

                        // Execute payment with decrypted key
                        // For non-custodial wallets, use the signing key to sign the transaction
                        // For custodial wallets, use system signing authority
                        await PaymentService.processPayment({
                            paymentId: payment.id,
                            userId: payment.user_id,
                            amount: payment.amount,
                            asset: payment.asset,
                            recipientTag: payment.recipient_tag,
                            signingKey: signingKey,
                            idempotencyKey: idempotencyKeyFor(payment),
                        });

                        // Secrets are automatically cleared after callback execution
                    });

                    // Mark as completed and clear any prior failure tracking. If our
                    // claim expired and another replica recovered the row, that
                    // replica owns completion (its payment replays via the key).
                    if (!(await ScheduledPayment.completeClaim(payment.id, WORKER_ID))) {
                        console.warn(`⚠️ Scheduler: claim on payment #${payment.id} was lost; leaving completion to its new owner`);
                        continue;
                    }

                    // Notify the user (creates the in-app record and sends the push)
                    await NotificationService.sendToUser(payment.user_id,
                        "Scheduled Payment Executed",
                        `Your scheduled payment of ${payment.amount} ${payment.asset} to @${payment.recipient_tag} has been executed successfully.`,
                        { type: "payment_notifications", scheduled_payment_id: String(payment.id) }
                    ).catch(err => console.error('Notification error (scheduled payment success):', err.message));

                    processed++;
                    console.log(`✅ Scheduler: executed payment #${payment.id}`);
                } catch (error) {
                    console.error(
                        `❌ Scheduler: failed to execute payment #${payment.id}:`,
                        error.message
                    );

                    // Log failed key access attempt
                    if (error.message.includes("No signing keys")) {
                        await AuditLog.create({
                            userId: payment.user_id,
                            action: "key_access_failed",
                            resource: "scheduled_payment",
                            resourceId: payment.id,
                            details: { reason: "no_signing_keys_found", payment_id: payment.id },
                            method: "SCHEDULER",
                            endpoint: "/scheduler/execute-payment",
                            statusCode: 422,
                        });
                    }

                    const updatedPayment = await ScheduledPayment.recordFailure(payment.id, error.message);
                    const isPaused = updatedPayment.status === "paused";

                    const baseMessage = `Your scheduled payment of ${payment.amount} ${payment.asset} to @${payment.recipient_tag} failed: ${error.message}`;
                    const title = isPaused
                        ? "Scheduled Payment Paused After Repeated Failures"
                        : "Scheduled Payment Failed";
                    const body = isPaused
                        ? `${baseMessage}. After ${updatedPayment.failure_count} failed attempts, this scheduled payment has been paused — resume it from the app once the issue is resolved.`
                        : `${baseMessage}. We'll try again shortly.`;

                    // Notify the user about the failure (in-app + push)
                    await NotificationService.sendToUser(payment.user_id, title, body, {
                        type: "payment_notifications",
                        scheduled_payment_id: String(payment.id),
                        priority: isPaused ? "high" : "normal",
                    }).catch(err => console.error('Notification error (scheduled payment failure):', err.message));

                    // Email is the durable channel for a failure the user might miss in-app
                    if (payment.user_email) {
                        await sendEmail(payment.user_email, title, body)
                            .catch(err => console.error('Email error (scheduled payment failure):', err.message));
                    }

                    failed++;
                }
            }

            return { processed, failed, total: duePayments.length };
        },
        {
            connection: redisConnection,
            concurrency: 1,
        }
    )
    : null;
attachRedisErrorAlert(executionWorker, "scheduled-payment-executor-worker");

// ========== Notification Worker ==========
// Runs every 5 minutes — sends reminders for payments due within 30 minutes

export const notificationWorker = redisConnection
    ? new Worker(
        "scheduled-payment-notifier",
        async (job) => {
            console.log(`🔔 Notifier: checking for upcoming payments...`);

            const upcomingPayments =
                await ScheduledPayment.getUpcomingForNotification(30);

            if (upcomingPayments.length === 0) {
                console.log(`🔔 Notifier: no upcoming payments to notify about.`);
                return { notified: 0 };
            }

            console.log(
                `🔔 Notifier: found ${upcomingPayments.length} upcoming payment(s).`
            );
            let notified = 0;

            for (const payment of upcomingPayments) {
                try {
                    const scheduledTime = new Date(
                        payment.scheduled_at
                    ).toLocaleString();

                    await Notification.create({
                        user_id: payment.user_id,
                        title: "Upcoming Scheduled Payment",
                        body: `Reminder: Your payment of ${payment.amount} ${payment.asset} to @${payment.recipient_tag} is scheduled for ${scheduledTime}. Cancel now if needed.`,
                    });

                    // Mark as notified so we don't send duplicate notifications
                    await ScheduledPayment.update(payment.id, {
                        notified_at: new Date(),
                    });

                    notified++;
                    console.log(
                        `🔔 Notifier: sent reminder for payment #${payment.id}`
                    );
                } catch (error) {
                    console.error(
                        `❌ Notifier: failed to notify for payment #${payment.id}:`,
                        error.message
                    );
                }
            }

            return { notified, total: upcomingPayments.length };
        },
        {
            connection: redisConnection,
            concurrency: 1,
        }
    )
    : null;
attachRedisErrorAlert(notificationWorker, "scheduled-payment-notifier-worker");

// ========== Register Repeatable Jobs ==========

async function registerRepeatableJobs() {
    if (schedulerQueue) {
        await schedulerQueue.add(
            "execute-due-payments",
            {},
            {
                repeat: { every: 60_000 },
                removeOnComplete: 50,
                removeOnFail: false,
            }
        );
        console.log("⏰ Scheduled payment executor registered (every 60s)");
    }

    if (notifierQueue) {
        await notifierQueue.add(
            "notify-upcoming-payments",
            {},
            {
                repeat: { every: 5 * 60_000 },
                removeOnComplete: 50,
                removeOnFail: false,
            }
        );
        console.log("🔔 Scheduled payment notifier registered (every 5min)");
    }

    if (apiKeyRotationQueue) {
        await apiKeyRotationQueue.add(
            "check-api-key-rotations",
            {},
            {
                repeat: { every: 60 * 60_000 },
                removeOnComplete: 50,
                removeOnFail: false,
            }
        );
        console.log("🔑 API key rotation worker registered (every 1h)");
    }

    await registerReconciliationJob();
    await registerBackupJob();
}

// Register jobs on startup
registerRepeatableJobs().catch((err) => {
    console.error("❌ Failed to register scheduled payment jobs:", err.message);
});

// ========== Worker Event Handlers ==========

if (executionWorker) {
    executionWorker.on("completed", (job, result) => {
        console.log(
            `✅ Execution worker completed job ${job.id}:`,
            JSON.stringify(result)
        );
    });
    executionWorker.on("failed", (job, err) => {
        console.error(
            `💥 Execution worker failed job ${job.id}:`,
            err.message
        );
    });
} else {
    console.warn(
        "⚠️ Scheduled payment execution worker not available (Redis not connected)"
    );
}

if (notificationWorker) {
    notificationWorker.on("completed", (job, result) => {
        console.log(
            `✅ Notification worker completed job ${job.id}:`,
            JSON.stringify(result)
        );
    });
    notificationWorker.on("failed", (job, err) => {
        console.error(
            `💥 Notification worker failed job ${job.id}:`,
            err.message
        );
    });
} else {
    console.warn(
        "⚠️ Scheduled payment notification worker not available (Redis not connected)"
    );
}
