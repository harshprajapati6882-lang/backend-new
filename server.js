const express = require('express');
const cors = require('cors');
const axios = require('axios');
const mongoose = require('mongoose');
const crypto = require('crypto');
mongoose.set('bufferCommands', false);

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

/* =========================
   🔥 MONGODB CONNECTION
========================= */ 
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://harshprajapati6882_db_user:mbyjv1uPdKtLBz1l@devanush.tqknxqf.mongodb.net/smm-panel?retryWrites=true&w=majority';

mongoose.connect(MONGODB_URI, {
  serverSelectionTimeoutMS: 30000,
})
.then(() => {
  console.log('✅ MongoDB Connected Successfully');
})
.catch(err => {
  console.error('❌ MongoDB Connection Error:', err);
});

/* =========================
   🔥 MONGODB SCHEMAS
========================= */
const RunSchema = new mongoose.Schema({
  id: { type: Number, required: true, index: true },
  schedulerOrderId: { type: String, required: true, index: true },
  label: { type: String, required: true },
  apiUrl: { type: String, required: true },
  apiKey: { type: String, required: true },
  service: { type: String, required: true },
  link: { type: String, required: true },
  quantity: { type: Number, required: true },
  time: { type: Date, required: true },
  done: { type: Boolean, default: false },
  status: { type: String, default: 'pending', index: true },
  smmOrderId: { type: Number, default: null },
  createdAt: { type: Date, default: Date.now },
  executedAt: { type: Date, default: null },
  error: { type: String, default: null },
  comments: { type: String, default: null },
  // 🔥 Execution lock fields
  executionLock: { type: String, default: null },
  lockedAt: { type: Date, default: null },
    // 🔥 NEW: Track which scheduler tick claimed this run
  claimedByTick: { type: String, default: null },
  // 🔥 NEW: Retry tracking for "active order" rejections
  retryCount: { type: Number, default: 0 },
  originalScheduledTime: { type: Date, default: null },
  actualExecutedAt: { type: Date, default: null },
  // 🔥 NEW: Per-service minimum quantity from the SMM panel
  serviceMin: { type: Number, default: null },
});

// 🔥 COMPOUND INDEXES for atomic operations
RunSchema.index({ status: 1, time: 1 });
RunSchema.index({ _id: 1, status: 1 }); 
RunSchema.index({ schedulerOrderId: 1, status: 1 });

const OrderSchema = new mongoose.Schema({
  schedulerOrderId: { type: String, required: true, unique: true, index: true },
  name: { type: String, required: true },
  link: { type: String, required: true },
  status: { type: String, default: 'pending' },
  totalRuns: { type: Number, required: true },
  completedRuns: { type: Number, default: 0 },
  runStatuses: [{ type: String }],
  createdAt: { type: Date, default: Date.now },
  lastUpdatedAt: { type: Date, default: Date.now },
});

const NotificationSchema = new mongoose.Schema({
  type: { type: String, required: true, index: true },
  severity: { type: String, required: true, index: true },
  title: { type: String, required: true },
  message: { type: String, required: true },
  schedulerOrderId: { type: String, default: null, index: true },
  runId: { type: String, default: null },
  label: { type: String, default: null },
  smmOrderId: { type: Number, default: null },
  read: { type: Boolean, default: false, index: true },
  createdAt: { type: Date, default: Date.now, index: true },
});

NotificationSchema.index({ createdAt: -1 });
NotificationSchema.index({ read: 1, createdAt: -1 });

const Run = mongoose.model('Run', RunSchema);
const Order = mongoose.model('Order', OrderSchema);
const Notification = mongoose.model('Notification', NotificationSchema);

/* =========================
   🔥 NOTIFICATION HELPER
========================= */
async function createNotification({ type, severity, title, message, schedulerOrderId, runId, label, smmOrderId }) {
  try {
    const notif = new Notification({
      type,
      severity,
      title,
      message,
      schedulerOrderId: schedulerOrderId || null,
      runId: runId || null,
      label: label || null,
      smmOrderId: smmOrderId || null,
      read: false,
      createdAt: new Date(),
    });
    await notif.save();
    console.log(`[NOTIFICATION] ${severity.toUpperCase()}: ${title}`);
  } catch (err) {
    console.error('[NOTIFICATION] Failed to save:', err.message);
  }
}

/* =========================
   MINIMUM VIEWS PER RUN
========================= */
let MIN_VIEWS_PER_RUN = 100;

/* =========================
   🔥 FIX 1: SCHEDULER LOCK
   Prevents concurrent scheduler ticks
========================= */
let isSchedulerRunning = false;
let schedulerTickId = 0;

/* =========================
   🔥 FIX 2: PROPER QUEUE SYSTEM
   Each queue tracks items by MongoDB _id
   No in-memory dedup sets needed — MongoDB is the source of truth
========================= */
let viewsQueue = [];
let likesQueue = [];
let sharesQueue = [];
let savesQueue = [];
let commentsQueue = [];
let repostsQueue = [];

let isExecutingViews = false;
let isExecutingLikes = false;
let isExecutingShares = false;
let isExecutingSaves = false;
let isExecutingComments = false;
let isExecutingReposts = false;

/* =========================
   PLACE ORDER
========================= */
async function placeOrder({ apiUrl, apiKey, service, link, quantity, comments }) {
  const params = new URLSearchParams({
    key: apiKey,
    action: 'add',
    service: String(service),
    link: String(link),
    quantity: String(quantity),
  });

  if (comments) {
    params.append('comments', comments);
  }

  const response = await axios.post(apiUrl, params.toString(), {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    timeout: 45000, // 🔥 Increased to 45 seconds
  });

  // 🔥 Handle HTML error responses (502 Bad Gateway pages)
  if (typeof response.data === 'string' && response.data.includes('<html')) {
    throw new Error(`HTTP ${response.status} Bad Gateway`);
  }

  return response.data;
}

/* =========================
   🔥 CANCEL ORDER ON PROVIDER
========================= */
async function cancelProviderOrder(apiUrl, apiKey, smmOrderId) {
  try {
    const params = new URLSearchParams({
      key: apiKey,
      action: 'cancel',
      orders: String(smmOrderId),
    });
    const response = await axios.post(apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 15000,
    });
    const result = response.data;
    if (Array.isArray(result) && result[0]) {
      if (result[0].cancel === 1 || result[0].cancel === '1') {
        return { success: true };
      }
      return { success: false, error: result[0].cancel?.error || 'Provider refused cancel' };
    }
    return { success: false, error: 'Invalid response format' };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/* =========================
   🔥 CHECK PROVIDER STATUS OF A SINGLE ORDER
========================= */
async function checkSingleProviderStatus(apiUrl, apiKey, smmOrderId) {
  try {
    const params = new URLSearchParams({
      key: apiKey,
      action: 'status',
      order: String(smmOrderId),
    });
    const response = await axios.post(apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 15000,
    });
    return response.data?.status || null;
  } catch (err) {
    return null;
  }
}

/* =========================
   ADD RUNS TO DATABASE
   🔥 STAGGERED EXECUTION:
   - Views execute at exact scheduled time (T+0)
   - Likes execute T+3 to T+7 minutes later (random)
   - Shares execute T+8 to T+12 minutes later (random)
   - Saves execute T+10 to T+15 minutes later (random)
   - Comments execute T+12 to T+18 minutes later (random)
   This makes engagement look organic — views first, then reactions follow naturally
========================= */
function getServiceDelay(label) {
  // 🔥 Wider, more organic delay ranges
  // Some users react in seconds, some in hours — this mimics that spread
  switch (label) {
    case 'VIEWS':
      return 0;
    case 'LIKES':
      // 2-90 minutes: some likes come fast, some come much later
      return (2 + Math.random() * 88) * 60 * 1000;
    case 'SHARES':
      // 5-120 minutes: shares are rarer and more spread out
      return (5 + Math.random() * 115) * 60 * 1000;
    case 'SAVES':
      // 3-150 minutes: saves happen at unpredictable times
      return (3 + Math.random() * 147) * 60 * 1000;
    case 'COMMENTS':
      // 8-180 minutes: comments take longest — people think before commenting
      return (8 + Math.random() * 172) * 60 * 1000;
    case 'REPOSTS':
      // 5-90 minutes
      return (5 + Math.random() * 85) * 60 * 1000;
    default:
      return 0;
  }
}

async function addRuns(services, baseConfig, schedulerOrderId) {
  const runsForOrder = [];

  for (const [key, serviceConfig] of Object.entries(services)) {
    if (!serviceConfig) continue;

    const label = key.toUpperCase();

    for (const run of serviceConfig.runs) {
      let quantity;

      if (label === 'VIEWS') {
        if (!run.quantity || run.quantity < 100) continue;
        quantity = run.quantity;
      } else if (label === 'COMMENTS') {
        if (!run.comments) continue;
        let lines = run.comments
          .split('\n')
          .map(c => c.trim())
          .filter(c => c.length > 0);
        if (lines.length < 10) continue;
        if (lines.length > 15) {
          lines = lines.sort(() => Math.random() - 0.5).slice(0, 15);
        }
        run.comments = lines.join('\n');
        quantity = lines.length;
      } else {
        if (!run.quantity || run.quantity <= 0) continue;
        quantity = run.quantity;
      }

      // 🔥 STAGGERED: Calculate delayed time based on service type
      const baseTime = new Date(run.time).getTime();
      const delay = getServiceDelay(label);
      const staggeredTime = new Date(baseTime + delay);

      console.log(`[ADD RUN] ${label} qty=${quantity} | base=${new Date(baseTime).toISOString()} | delay=${Math.round(delay / 60000)}min | actual=${staggeredTime.toISOString()}`);

                  // 🔥 Use per-service API if provided, otherwise fall back to baseConfig
      const serviceApiUrl = serviceConfig.apiUrl || baseConfig.apiUrl;
      const serviceApiKey = serviceConfig.apiKey || baseConfig.apiKey;
      const serviceMin = serviceConfig.serviceMin || null; // 🔥 Per-service minimum from SMM panel

          const runData = new Run({
        id: Date.now() + Math.random(),
        schedulerOrderId,
        label,
        apiUrl: serviceApiUrl,
        apiKey: serviceApiKey,
        service: serviceConfig.serviceId,
        link: baseConfig.link,
        quantity: quantity,
        time: staggeredTime, // 🔥 Use staggered time instead of original
        done: false,
        status: 'pending',
        smmOrderId: null,
        createdAt: new Date(),
        executedAt: null,
        error: null,
        comments: run.comments || null,
        executionLock: null,
        lockedAt: null,
        claimedByTick: null,
        serviceMin: serviceMin, // 🔥 Store per-service minimum from SMM panel
      });

      await runData.save();
      runsForOrder.push(runData);
    }
  }

  return runsForOrder;
}

/* =========================
   🔥 FIX 3: TRULY ATOMIC CLAIM
   Uses MongoDB findOneAndUpdate with ALL conditions
   Returns null if ANY other process already claimed it
========================= */
async function atomicClaimRun(runId, tickId) {
  const result = await Run.findOneAndUpdate(
    {
      _id: runId,
      status: 'pending',           // 🔥 MUST be pending
      executionLock: null,         // 🔥 MUST NOT be locked by anyone
      claimedByTick: null,         // 🔥 MUST NOT be claimed by any tick
    },
    {
      $set: {
        status: 'queued',
        claimedByTick: tickId,
        executionLock: `claim-${tickId}`,
        lockedAt: new Date(),
      },
    },
    {
      new: true,
    }
  );

  return result; // null = someone else got it first
}

/* =========================
   🔥 FIX 4: ATOMIC EXECUTE LOCK
   Transitions from queued → processing
   Only succeeds if still in queued state with matching tick
========================= */
async function atomicExecuteLock(runId, tickId) {
  const lockId = `exec-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
  
  const result = await Run.findOneAndUpdate(
    {
      _id: runId,
      status: 'queued',              // 🔥 MUST be queued
      claimedByTick: tickId,         // 🔥 MUST be claimed by this tick
    },
    {
      $set: {
        status: 'processing',
        executionLock: lockId,
        lockedAt: new Date(),
      },
    },
    {
      new: true,
    }
  );

  return result;
}

/* =========================
   🔥 FIX 5: EXECUTE RUN (BULLETPROOF + SELF-HEALING)
   - Checks provider status before executing
   - VIEWS: waits 20 min if previous stuck, then cancels and executes
   - LIKES/SHARES/SAVES/COMMENTS: immediately cancels previous if stuck
   - Failed run NEVER affects next runs
========================= */
async function executeRun(run, tickId) {
  const runIdStr = run._id.toString();

  try {
    // 🔥 STEP 1: Atomic lock from queued → processing
    const lockedRun = await atomicExecuteLock(run._id, tickId);

    if (!lockedRun) {
      console.log(`[SKIP] Run ${runIdStr} - could not acquire execute lock`);
      return;
    }

    // 🔥 STEP 2: Check if order is cancelled
    const order = await Order.findOne({ schedulerOrderId: lockedRun.schedulerOrderId });
    if (!order || order.status === 'cancelled') {
      console.log(`[SKIP] Order cancelled → run skipped`);
      await Run.updateOne(
        { _id: run._id, status: 'processing' },
        { $set: { status: 'cancelled', done: true } }
      );
      await updateOrderStatus(lockedRun.schedulerOrderId);
      return;
    }

    if (!lockedRun.quantity || lockedRun.quantity <= 0) {
      await Run.updateOne(
        { _id: run._id, status: 'processing' },
        { $set: { status: 'failed', error: 'Zero quantity', done: true } }
      );
      await updateOrderStatus(lockedRun.schedulerOrderId);
      return;
    }

        // 🔥 STEP 3: Minimum quantity safety check — use per-run stored minimum if available
    const FALLBACK_MINIMUMS = { VIEWS: 100, LIKES: 10, SHARES: 10, SAVES: 10, COMMENTS: 10, REPOSTS: 10 };
    const serviceMin = lockedRun.serviceMin || FALLBACK_MINIMUMS[lockedRun.label] || 1;
    if (lockedRun.quantity < serviceMin) {
      console.log(`[${lockedRun.label}] SKIP: quantity ${lockedRun.quantity} below service minimum ${serviceMin}`);
      await Run.findOneAndUpdate(
        { _id: run._id, status: 'processing' },
        { $set: { status: 'failed', error: `Quantity ${lockedRun.quantity} is below minimum ${serviceMin} for ${lockedRun.label}`, done: true } }
      );
      await updateOrderStatus(lockedRun.schedulerOrderId);
      return;
    }

         // =========================================================
    // 🔥 STEP 3.5: For LIKES/SHARES/SAVES/COMMENTS
    // Check if VIEWS run for the same time slot is completed first
    // If VIEWS is still pending/retrying, postpone this run too
    // =========================================================
    if (lockedRun.label !== 'VIEWS') {
      // Find the VIEWS run for this same order that was scheduled
      // closest to (but before) this engagement run's original time
      const thisRunTime = lockedRun.originalScheduledTime || lockedRun.time;

            const correspondingViewsRun = await Run.findOne({
        schedulerOrderId: lockedRun.schedulerOrderId,
        label: 'VIEWS',
        // 🔥 FIX: Find VIEWS run whose originalScheduledTime OR time
        // is within 20 minutes BEFORE this engagement run's original time
        // This correctly links engagement runs to their corresponding VIEWS run
        $or: [
          // Match by originalScheduledTime (most accurate when runs have been rescheduled)
          {
            originalScheduledTime: {
              $gte: new Date(thisRunTime.getTime() - 20 * 60 * 1000),
              $lte: new Date(thisRunTime.getTime() + 1 * 60 * 1000),
            }
          },
          // Fallback: match by current time field
          {
            originalScheduledTime: null,
            time: {
              $gte: new Date(thisRunTime.getTime() - 20 * 60 * 1000),
              $lte: new Date(thisRunTime.getTime() + 1 * 60 * 1000),
            }
          }
        ]
      }).sort({ time: -1 });

      if (correspondingViewsRun) {
        const viewsStatus = correspondingViewsRun.status;

        // If VIEWS run is still pending/queued/processing (retrying or waiting)
        const viewsNotDoneYet = viewsStatus === 'pending' || viewsStatus === 'queued' || viewsStatus === 'processing';

        if (viewsNotDoneYet) {
          console.log(`[${lockedRun.label}] VIEWS run for this slot is still "${viewsStatus}". Postponing ${lockedRun.label} by 5 min...`);

          const postponeTime = new Date(Date.now() + 5 * 60 * 1000);
          await Run.findOneAndUpdate(
            { _id: run._id, status: 'processing' },
            {
              $set: {
                status: 'pending',
                time: postponeTime,
                executionLock: null,
                claimedByTick: null,
                lockedAt: null,
                originalScheduledTime: lockedRun.originalScheduledTime || lockedRun.time,
                error: `Waiting for VIEWS run to complete first. VIEWS status: ${viewsStatus}. Retrying at ${postponeTime.toISOString()}`,
              }
            }
          );

          return; // Exit — will retry in 5 min
        }

        // VIEWS completed — check if it completed successfully
        if (viewsStatus === 'failed') {
          console.log(`[${lockedRun.label}] VIEWS run for this slot FAILED. Proceeding with ${lockedRun.label} anyway.`);
          // Still proceed — better to place likes even if views failed
        }

                if (viewsStatus === 'cancelled') {
          // 🔥 Don't auto-cancel engagement. If user explicitly cancelled the order,
          // the order-level check already handles it. If VIEWS was provider-cancelled,
          // engagement should still be placed — it's a separate service.
          console.log(`[${lockedRun.label}] VIEWS run for this slot was cancelled. Proceeding with ${lockedRun.label} anyway (separate service).`);
          // Fall through to normal execution — do NOT cancel engagement
        }

        // VIEWS is completed — safe to proceed
        console.log(`[${lockedRun.label}] VIEWS run completed. Proceeding with ${lockedRun.label}.`);
      }
    }
    // =========================================================

        // =========================================================
    // 🔥 STEP 4: CHECK PREVIOUS ORDER STATUS (log only)
    // =========================================================
    const previousRun = await Run.findOne({
      link: lockedRun.link,
      label: lockedRun.label,
      schedulerOrderId: lockedRun.schedulerOrderId,
      status: 'completed',
      smmOrderId: { $ne: null },
      _id: { $ne: lockedRun._id },
    }).sort({ executedAt: -1 });

    if (previousRun && previousRun.smmOrderId) {
      const providerStatus = await checkSingleProviderStatus(
        previousRun.apiUrl,
        previousRun.apiKey,
        previousRun.smmOrderId
      );
      console.log(`[${lockedRun.label}] Previous order #${previousRun.smmOrderId} status: ${providerStatus}`);
    }
    // =========================================================

    // 🔥 STEP 5: Place the order
    console.log(`[${lockedRun.label}] Executing run #${lockedRun.id}, quantity: ${lockedRun.quantity}`);

    let payload = {
      apiUrl: lockedRun.apiUrl,
      apiKey: lockedRun.apiKey,
      service: lockedRun.service,
      link: lockedRun.link,
    };

    if (lockedRun.label === 'COMMENTS') {
      payload.comments = lockedRun.comments;
      payload.quantity = lockedRun.quantity;
    } else {
      payload.quantity = lockedRun.quantity;
    }

    const result = await placeOrder(payload);

    if (result?.order) {
      console.log(`[${lockedRun.label}] SUCCESS - SMM Order ID: ${result.order}`);
      const completed = await Run.findOneAndUpdate(
        { _id: run._id, status: 'processing' },
        {
          $set: {
            done: true,
            status: 'completed',
            smmOrderId: result.order,
            executedAt: new Date(),
            actualExecutedAt: new Date(),
          }
        },
        { new: true }
      );
      if (!completed) {
        console.warn(`[${lockedRun.label}] WARNING: Run completed but status update failed`);
      }
    } else {
      console.error(`[${lockedRun.label}] FAILED`, result);
      const errorMsg = result?.error || 'Unknown error';

      // 🔥 Is this an "active order" rejection from provider?
      const isActiveOrderError = errorMsg.toLowerCase().includes('active order') ||
        errorMsg.toLowerCase().includes('please wait');

      if (isActiveOrderError) {
        const currentRetryCount = lockedRun.retryCount || 0;
        const MAX_RETRIES = 6; // 6 × 5 min = 30 min max

        if (currentRetryCount < MAX_RETRIES) {
          // 🔥 Reschedule +5 minutes and retry
          const retryTime = new Date(Date.now() + 5 * 60 * 1000);
          console.log(`[${lockedRun.label}] Active order conflict. Retry ${currentRetryCount + 1}/${MAX_RETRIES} at ${retryTime.toISOString()}`);

                    // 🔥 FIX: Check if order was cancelled before rescheduling
          const orderCheckBeforeRetry = await Order.findOne({ schedulerOrderId: lockedRun.schedulerOrderId });
          if (!orderCheckBeforeRetry || orderCheckBeforeRetry.status === 'cancelled') {
            console.log(`[${lockedRun.label}] Order cancelled — not rescheduling retry.`);
            await Run.findOneAndUpdate(
              { _id: run._id, status: 'processing' },
              { $set: { status: 'cancelled', done: true } }
            );
            return;
          }

          await Run.findOneAndUpdate(
            { _id: run._id, status: 'processing' },
            {
              $set: {
                status: 'pending',
                time: retryTime,
                executionLock: null,
                claimedByTick: null,
                lockedAt: null,
                retryCount: currentRetryCount + 1,
                originalScheduledTime: currentRetryCount === 0 ? lockedRun.time : lockedRun.originalScheduledTime,
                error: `Retry ${currentRetryCount + 1}/${MAX_RETRIES}: Provider has active order for this link. Retrying at ${retryTime.toISOString()}`,
              }
            }
          );

          await createNotification({
            type: 'run_retrying',
            severity: 'warning',
            title: `${lockedRun.label} run rescheduled`,
            message: `${lockedRun.label} run retry ${currentRetryCount + 1}/${MAX_RETRIES}. Provider has active order for this link. Next attempt at ${retryTime.toLocaleTimeString()}.`,
            schedulerOrderId: lockedRun.schedulerOrderId,
            runId: run._id.toString(),
            label: lockedRun.label,
          });

          // Return early — run is still alive as pending
          return;

        } else {
          // 🔥 MAX RETRIES REACHED — check actual provider status
          console.log(`[${lockedRun.label}] Max retries (${MAX_RETRIES}) reached. Checking provider status...`);

          const previousRunForForce = await Run.findOne({
            link: lockedRun.link,
            label: lockedRun.label,
            schedulerOrderId: lockedRun.schedulerOrderId,
            status: 'completed',
            smmOrderId: { $ne: null },
            _id: { $ne: lockedRun._id },
          }).sort({ executedAt: -1 });

          if (previousRunForForce && previousRunForForce.smmOrderId) {
            const statusNow = await checkSingleProviderStatus(
              previousRunForForce.apiUrl,
              previousRunForForce.apiKey,
              previousRunForForce.smmOrderId
            );

            console.log(`[${lockedRun.label}] Status after max retries: ${statusNow}`);

            // 🔥 Partial/Completed/Cancelled = link is FREE (provider gave up)
            const isLinkFree = statusNow === 'Partial' || statusNow === 'Completed' || statusNow === 'Cancelled';
            const isStillBlocked = statusNow === 'In progress' || statusNow === 'Processing' || statusNow === 'Pending';

                       if (isLinkFree) {
              console.log(`[${lockedRun.label}] Previous order is "${statusNow}" — link is free. Retrying in 30 sec.`);
              // 🔥 FIX: Check cancellation before retry
              const orderCheckFree = await Order.findOne({ schedulerOrderId: lockedRun.schedulerOrderId });
              if (!orderCheckFree || orderCheckFree.status === 'cancelled') {
                await Run.findOneAndUpdate({ _id: run._id, status: 'processing' }, { $set: { status: 'cancelled', done: true } });
                return;
              }
              const immediateRetry = new Date(Date.now() + 30 * 1000);
              await Run.findOneAndUpdate(
                { _id: run._id, status: 'processing' },
                {
                  $set: {
                    status: 'pending',
                    time: immediateRetry,
                    executionLock: null,
                    claimedByTick: null,
                    lockedAt: null,
                    retryCount: 0,
                    error: null,
                  }
                }
              );
              return;

            } else if (isStillBlocked) {
              // Still blocked after 30 min — give up
              console.log(`[${lockedRun.label}] Still blocked after ${MAX_RETRIES} retries. Marking as failed.`);
              await Run.findOneAndUpdate(
                { _id: run._id, status: 'processing' },
                {
                  $set: {
                    status: 'failed',
                    error: `Max retries (${MAX_RETRIES}) reached. Previous order #${previousRunForForce.smmOrderId} still "${statusNow}" after 30 min.`,
                    done: true,
                  }
                }
              );
              await createNotification({
                type: 'run_failed',
                severity: 'critical',
                title: `${lockedRun.label} run failed after max retries`,
                message: `${lockedRun.label} run waited 30 min but previous order #${previousRunForForce.smmOrderId} is still "${statusNow}". Run marked as failed.`,
                schedulerOrderId: lockedRun.schedulerOrderId,
                runId: run._id.toString(),
                label: lockedRun.label,
                smmOrderId: previousRunForForce.smmOrderId,
              });

                       } else {
              // Unknown status — retry in 30 seconds as safe fallback
              // 🔥 FIX: Check cancellation before retry
              const orderCheckFallback = await Order.findOne({ schedulerOrderId: lockedRun.schedulerOrderId });
              if (!orderCheckFallback || orderCheckFallback.status === 'cancelled') {
                await Run.findOneAndUpdate({ _id: run._id, status: 'processing' }, { $set: { status: 'cancelled', done: true } });
                return;
              }
              const fallbackRetry = new Date(Date.now() + 30 * 1000);
              await Run.findOneAndUpdate(
                { _id: run._id, status: 'processing' },
                {
                  $set: {
                    status: 'pending',
                    time: fallbackRetry,
                    executionLock: null,
                    claimedByTick: null,
                    lockedAt: null,
                    retryCount: 0,
                    error: null,
                  }
                }
              );
              return;
            }
          } else {
            // No previous run found — just fail
            await Run.findOneAndUpdate(
              { _id: run._id, status: 'processing' },
              { $set: { status: 'failed', error: errorMsg, done: true } }
            );
            await createNotification({
              type: 'run_failed',
              severity: 'critical',
              title: lockedRun.label + ' run failed',
              message: lockedRun.label + ' run failed: ' + errorMsg,
              schedulerOrderId: lockedRun.schedulerOrderId,
              runId: run._id.toString(),
              label: lockedRun.label,
            });
          }
        }
            } else {
        // 🔥 Check if this is a retryable provider error (502/503/504/timeout/network)
        const isRetryableError =
          errorMsg.includes('502') ||
          errorMsg.includes('503') ||
          errorMsg.includes('504') ||
          errorMsg.toLowerCase().includes('timeout') ||
          errorMsg.toLowerCase().includes('econnreset') ||
          errorMsg.toLowerCase().includes('econnrefused') ||
          errorMsg.toLowerCase().includes('network') ||
          errorMsg.toLowerCase().includes('bad gateway') ||
          errorMsg.toLowerCase().includes('service unavailable');

        if (isRetryableError) {
          const currentRetryCount = lockedRun.retryCount || 0;
          const MAX_RETRIES = 4; // 4 × 5 min = 20 min max for provider errors

          if (currentRetryCount < MAX_RETRIES) {
            const retryTime = new Date(Date.now() + 5 * 60 * 1000);
            console.log(`[${lockedRun.label}] Provider error (${errorMsg.slice(0, 50)}). Retry ${currentRetryCount + 1}/${MAX_RETRIES} at ${retryTime.toISOString()}`);

            // Check if order was cancelled before rescheduling
            const orderCheckRetry = await Order.findOne({ schedulerOrderId: lockedRun.schedulerOrderId });
            if (!orderCheckRetry || orderCheckRetry.status === 'cancelled') {
              await Run.findOneAndUpdate(
                { _id: run._id, status: 'processing' },
                { $set: { status: 'cancelled', done: true } }
              );
              return;
            }

            await Run.findOneAndUpdate(
              { _id: run._id, status: 'processing' },
              {
                $set: {
                  status: 'pending',
                  time: retryTime,
                  executionLock: null,
                  claimedByTick: null,
                  lockedAt: null,
                  retryCount: currentRetryCount + 1,
                  originalScheduledTime: currentRetryCount === 0 ? lockedRun.time : lockedRun.originalScheduledTime,
                  error: `Retry ${currentRetryCount + 1}/${MAX_RETRIES}: Provider error - ${errorMsg.slice(0, 100)}. Retrying at ${retryTime.toISOString()}`,
                }
              }
            );

            await createNotification({
              type: 'run_retrying',
              severity: 'warning',
              title: `${lockedRun.label} run rescheduled (provider error)`,
              message: `${lockedRun.label} run retry ${currentRetryCount + 1}/${MAX_RETRIES}. Provider returned: ${errorMsg.slice(0, 80)}. Next attempt at ${retryTime.toLocaleTimeString()}.`,
              schedulerOrderId: lockedRun.schedulerOrderId,
              runId: run._id.toString(),
              label: lockedRun.label,
            });

            return; // Don't mark as failed — will retry
          }
        }

        // 🔥 Normal failure (not retryable or max retries reached)
        await Run.findOneAndUpdate(
          { _id: run._id, status: 'processing' },
          { $set: { status: 'failed', error: errorMsg, done: true } }
        );
        await createNotification({
          type: 'run_failed',
          severity: 'critical',
          title: lockedRun.label + ' run failed',
          message: lockedRun.label + ' run (qty: ' + lockedRun.quantity + ') failed: ' + errorMsg,
          schedulerOrderId: lockedRun.schedulerOrderId,
          runId: run._id.toString(),
          label: lockedRun.label,
        });
      }
    }

    } catch (err) {
    console.error(`[${run.label}] ERROR`, err.response?.data || err.message);

    // 🔥 Build error message — handle HTML responses (502 pages)
    let errorMsg = err.message;
    if (err.response?.data) {
      if (typeof err.response.data === 'string' && err.response.data.includes('<html')) {
        errorMsg = `HTTP ${err.response.status} Bad Gateway`;
      } else {
        errorMsg = err.response.data?.error || err.response.data || err.message;
      }
    }

    // 🔥 Check if retryable network/provider error
    const isRetryableNetworkError =
      err.code === 'ECONNABORTED' ||
      err.code === 'ECONNRESET' ||
      err.code === 'ECONNREFUSED' ||
      err.code === 'ETIMEDOUT' ||
      err.message?.toLowerCase().includes('timeout') ||
      (err.response?.status >= 500 && err.response?.status <= 599);

    if (isRetryableNetworkError && run?._id) {
      const currentRetryCount = run.retryCount || 0;
      const MAX_RETRIES = 4;

      if (currentRetryCount < MAX_RETRIES) {
        const retryTime = new Date(Date.now() + 5 * 60 * 1000);
        console.log(`[${run.label}] Network error, retry ${currentRetryCount + 1}/${MAX_RETRIES} at ${retryTime.toISOString()}`);

        // Check if order was cancelled
        const orderCheck = await Order.findOne({ schedulerOrderId: run.schedulerOrderId });
        if (!orderCheck || orderCheck.status === 'cancelled') {
          await Run.findOneAndUpdate(
            { _id: run._id, status: 'processing' },
            { $set: { status: 'cancelled', done: true } }
          );
          return;
        }

        await Run.findOneAndUpdate(
          { _id: run._id, status: 'processing' },
          {
            $set: {
              status: 'pending',
              time: retryTime,
              executionLock: null,
              claimedByTick: null,
              lockedAt: null,
              retryCount: currentRetryCount + 1,
              originalScheduledTime: currentRetryCount === 0 ? run.time : run.originalScheduledTime,
              error: `Retry ${currentRetryCount + 1}/${MAX_RETRIES}: ${errorMsg}. Retrying at ${retryTime.toISOString()}`,
            }
          }
        );

        await createNotification({
          type: 'run_retrying',
          severity: 'warning',
          title: `${run.label} run rescheduled (network error)`,
          message: `${run.label} run retry ${currentRetryCount + 1}/${MAX_RETRIES}. Error: ${errorMsg}. Next attempt at ${retryTime.toLocaleTimeString()}.`,
          schedulerOrderId: run.schedulerOrderId,
          runId: run?._id?.toString(),
          label: run.label,
        });

        return; // Will retry — don't fall through to failed
      }
    }

    // Not retryable or max retries reached — mark as failed
    if (run?._id) {
      await Run.findOneAndUpdate(
        { _id: run._id, status: 'processing' },
        { $set: { status: 'failed', error: String(errorMsg), done: true } }
      );
    }
    await createNotification({
      type: 'run_error',
      severity: 'critical',
      title: `${run.label} run error`,
      message: `${run.label} run (qty: ${run.quantity}) failed after retries: ${errorMsg}`,
      schedulerOrderId: run.schedulerOrderId,
      runId: run?._id?.toString(),
      label: run.label,
    });
  } finally {
    await updateOrderStatus(run.schedulerOrderId);
  }
}

/* =========================
   UPDATE ORDER STATUS
========================= */
async function updateOrderStatus(schedulerOrderId) {
  if (!schedulerOrderId) return;

  try {
    const orderRuns = await Run.find({ schedulerOrderId });
    const order = await Order.findOne({ schedulerOrderId });

    if (!order) return;

    const totalRuns = orderRuns.length;
    const completedRuns = orderRuns.filter(r => r.status === 'completed').length;
    const failedRuns = orderRuns.filter(r => r.status === 'failed').length;
    const processingRuns = orderRuns.filter(r => r.status === 'processing').length;
    const queuedRuns = orderRuns.filter(r => r.status === 'queued').length;
    const cancelledRuns = orderRuns.filter(r => r.status === 'cancelled').length;

    let newStatus;
    if (order.status === 'cancelled') {
      newStatus = 'cancelled'; // 🔥 Don't override manual cancellation
        } else if (completedRuns + failedRuns + cancelledRuns === totalRuns) {
      // All runs are done (one way or another)
      if (completedRuns === totalRuns) {
        newStatus = 'completed';
      } else if (failedRuns === totalRuns) {
        newStatus = 'failed';
      } else if (cancelledRuns === totalRuns) {
        newStatus = 'cancelled';
      } else if (cancelledRuns > 0 && completedRuns > 0) {
        // 🔥 FIX: Mix of completed + cancelled = completed (not running)
        newStatus = 'completed';
      } else {
        newStatus = 'completed';
      }
    } else if (processingRuns > 0 || queuedRuns > 0 || completedRuns > 0) {
      newStatus = 'running';
    } else {
      newStatus = 'pending';
    }

       // 🔥 NOTIFICATION: Order has failed runs
    if (newStatus === 'completed' && failedRuns > 0) {
      await createNotification({
        type: 'order_partial_failure',
        severity: 'warning',
        title: `Order completed with failures`,
        message: `Order ${schedulerOrderId}: ${completedRuns} completed, ${failedRuns} failed, ${cancelledRuns} cancelled out of ${totalRuns} total runs.`,
        schedulerOrderId,
      });
    }

    await Order.updateOne(
      { schedulerOrderId },
      {
        $set: {
          status: newStatus,
          completedRuns: completedRuns,
          totalRuns: totalRuns,
          lastUpdatedAt: new Date(),
          runStatuses: orderRuns.map(r => r.status),
        }
      }
    );
  } catch (err) {
    console.error(`[updateOrderStatus] Error for ${schedulerOrderId}:`, err.message);
  }
}

/* =========================
   🔥 FIX 6: QUEUE PROCESSORS (SIMPLIFIED)
   - No in-memory tracking needed
   - Each run carries its tickId for verification
   - MongoDB atomic operations prevent duplicates
========================= */
async function processQueue(queueName, queue, isExecutingFlag, setExecutingFlag) {
  if (isExecutingFlag() || queue.length === 0) return;

  setExecutingFlag(true);

  while (queue.length > 0) {
    const queueItem = queue.shift();
    
    if (!queueItem || !queueItem.run || !queueItem.tickId) {
      console.log(`[${queueName} QUEUE] Invalid queue item, skipping`);
      continue;
    }

    const { run, tickId } = queueItem;
    const runIdStr = run._id.toString();

    console.log(`[${queueName} QUEUE] Processing run ${runIdStr}, Remaining: ${queue.length}`);

    try {
      // 🔥 Re-verify from DB before executing
      const freshRun = await Run.findById(run._id);

      if (!freshRun) {
        console.log(`[${queueName} QUEUE] Run not found in DB, skipping`);
        continue;
      }

      // 🔥 Only execute if still in 'queued' state AND claimed by our tick
      if (freshRun.status !== 'queued') {
        console.log(`[${queueName} QUEUE] Run status is '${freshRun.status}' (not queued), skipping`);
        continue;
      }

      if (freshRun.claimedByTick !== tickId) {
        console.log(`[${queueName} QUEUE] Run claimed by different tick (${freshRun.claimedByTick} vs ${tickId}), skipping`);
        continue;
      }

      await executeRun(freshRun, tickId);
    } catch (err) {
      console.error(`[${queueName} QUEUE] Error processing run ${runIdStr}:`, err.message);
    }

    // 🔥 Delay between executions to avoid rate limiting
    if (queue.length > 0) {
      await new Promise(resolve => setTimeout(resolve, 8000));
    }
  }

  setExecutingFlag(false);
}

// Queue processor starters
function processViewsQueue() {
  processQueue('VIEWS', viewsQueue, () => isExecutingViews, (v) => { isExecutingViews = v; });
}
function processLikesQueue() {
  processQueue('LIKES', likesQueue, () => isExecutingLikes, (v) => { isExecutingLikes = v; });
}
function processSharesQueue() {
  processQueue('SHARES', sharesQueue, () => isExecutingShares, (v) => { isExecutingShares = v; });
}
function processSavesQueue() {
  processQueue('SAVES', savesQueue, () => isExecutingSaves, (v) => { isExecutingSaves = v; });
}
function processCommentsQueue() {
  processQueue('COMMENTS', commentsQueue, () => isExecutingComments, (v) => { isExecutingComments = v; });
}
function processRepostsQueue() {
  processQueue('REPOSTS', repostsQueue, () => isExecutingReposts, (v) => { isExecutingReposts = v; });
}

/* =========================
   🔥 FIX 7: MAIN SCHEDULER (BULLETPROOF)
   - Mutex lock prevents concurrent ticks
   - Each tick gets a unique ID
   - MongoDB atomic claims prevent any duplicates
========================= */
mongoose.connection.once('open', () => {
  console.log("🚀 Scheduler started after DB connected");

  // 🔥 STARTUP: Clean up stuck runs from previous server instance
  (async () => {
    try {
      // Reset any runs that were in-progress when server crashed
      const stuckProcessing = await Run.updateMany(
        { status: 'processing', executedAt: null },
        { $set: { status: 'pending', executionLock: null, lockedAt: null, claimedByTick: null } }
      );
      const stuckQueued = await Run.updateMany(
        { status: 'queued' },
        { $set: { status: 'pending', executionLock: null, lockedAt: null, claimedByTick: null } }
      );
      console.log(`[STARTUP] Reset ${stuckProcessing.modifiedCount} stuck processing runs`);
      console.log(`[STARTUP] Reset ${stuckQueued.modifiedCount} stuck queued runs`);
             // 🔥 NOTIFICATION: Server restart
      const totalReset = stuckProcessing.modifiedCount + stuckQueued.modifiedCount;
      if (totalReset > 0) {
        await createNotification({
          type: 'server_restart',
          severity: 'info',
          title: 'Server restarted',
          message: `Server restarted. Reset ${stuckProcessing.modifiedCount} processing + ${stuckQueued.modifiedCount} queued runs back to pending.`,
        });
      }
    } catch (err) {
      console.error('[STARTUP] Error cleaning stuck runs:', err);
    }
  })();

  setInterval(async () => {
    // 🔥 FIX: Mutex — skip if previous tick is still running
    if (isSchedulerRunning) {
      console.log('[SCHEDULER] Previous tick still running, skipping...');
      return;
    }

    isSchedulerRunning = true;
    schedulerTickId++;
    const tickId = `tick-${schedulerTickId}-${Date.now()}`;

    try {
      const now = new Date();
      let addedToQueue = { views: 0, likes: 0, shares: 0, saves: 0, comments: 0, reposts: 0 };

      // 🔥 FIX: Use MongoDB atomic findOneAndUpdate in a loop
      // Instead of find() then update(), we claim runs one-by-one atomically
      let claimedCount = 0;
      const MAX_CLAIMS_PER_TICK = 50; // Safety limit

      while (claimedCount < MAX_CLAIMS_PER_TICK) {
               // 🔥 Atomically find AND claim ONE pending run.
        // Free Render instances can sleep; when they wake, immediately catch up
        // every overdue run instead of waiting for a "quiet hours" window to pass.
        const claimedRun = await Run.findOneAndUpdate(
          {
            status: 'pending',
            time: { $lte: now },
            executionLock: null,
            claimedByTick: null,
          },
          {
            $set: {
              status: 'queued',
              claimedByTick: tickId,
              executionLock: `claim-${tickId}`,
              lockedAt: new Date(),
            },
          },
          {
            new: true,
            sort: { time: 1 }, // Process oldest first
          }
        );

        // No more pending runs to claim
        if (!claimedRun) break;

               // 🔥 Verify order exists before adding to queue
        const order = await Order.findOne({ schedulerOrderId: claimedRun.schedulerOrderId });
        if (!order) {
          // ⚠️ Order not found — DON'T cancel. Reset run so it retries next tick.
          // MongoDB might have had a momentary hiccup. Cancelling permanently is too aggressive.
          console.log(`[SCHEDULER] Order not found for ${claimedRun.schedulerOrderId} — resetting run, will retry`);
          await Run.updateOne(
            { _id: claimedRun._id },
            { $set: { status: 'pending', executionLock: null, lockedAt: null, claimedByTick: null } }
          );
          continue;
        }
        if (order.status === 'cancelled') {
          // Only cancel if order is EXPLICITLY cancelled by user
          await Run.updateOne(
            { _id: claimedRun._id },
            { $set: { status: 'cancelled', done: true } }
          );
          continue;
        }

        // 🔥 Add to appropriate queue with tickId
        const queueItem = { run: claimedRun, tickId };

        if (claimedRun.label === 'VIEWS') {
          viewsQueue.push(queueItem);
          addedToQueue.views++;
        } else if (claimedRun.label === 'LIKES') {
          likesQueue.push(queueItem);
          addedToQueue.likes++;
        } else if (claimedRun.label === 'SHARES') {
          sharesQueue.push(queueItem);
          addedToQueue.shares++;
        } else if (claimedRun.label === 'SAVES') {
          savesQueue.push(queueItem);
          addedToQueue.saves++;
               } else if (claimedRun.label === 'COMMENTS') {
          commentsQueue.push(queueItem);
          addedToQueue.comments++;
        } else if (claimedRun.label === 'REPOSTS') {
          repostsQueue.push(queueItem);
          addedToQueue.reposts++;
        }

        claimedCount++;
        console.log(`[SCHEDULER] Claimed ${claimedRun.label} run (qty: ${claimedRun.quantity}) [${tickId}]`);
      }

      const totalAdded = addedToQueue.views + addedToQueue.likes + addedToQueue.shares + addedToQueue.saves + addedToQueue.comments + addedToQueue.reposts;
      if (totalAdded > 0) {
               console.log(`[SCHEDULER] [${tickId}] Claimed ${totalAdded} runs - Views: ${addedToQueue.views}, Likes: ${addedToQueue.likes}, Shares: ${addedToQueue.shares}, Saves: ${addedToQueue.saves}, Comments: ${addedToQueue.comments}, Reposts: ${addedToQueue.reposts}`);
      }

      // 🔥 Start processors if needed
      if (viewsQueue.length > 0 && !isExecutingViews) processViewsQueue();
      if (likesQueue.length > 0 && !isExecutingLikes) processLikesQueue();
      if (sharesQueue.length > 0 && !isExecutingShares) processSharesQueue();
      if (savesQueue.length > 0 && !isExecutingSaves) processSavesQueue();
      if (commentsQueue.length > 0 && !isExecutingComments) processCommentsQueue();
      if (repostsQueue.length > 0 && !isExecutingReposts) processRepostsQueue();

            } catch (error) {
      console.error('[SCHEDULER] Error:', error);
    } finally {
      // 🔥 NOTIFICATION: Check for stuck runs
      try {
        const stuckProcessingRuns = await Run.find({
          status: 'processing',
          executedAt: null,
          lockedAt: { $lt: new Date(Date.now() - 25 * 60 * 1000) },
        });

        for (const stuckRun of stuckProcessingRuns) {
          await createNotification({
            type: 'run_stuck',
            severity: 'warning',
            title: stuckRun.label + ' run stuck in processing',
            message: stuckRun.label + ' run (qty: ' + stuckRun.quantity + ') has been processing for over 25 minutes without completing.',
            schedulerOrderId: stuckRun.schedulerOrderId,
            runId: stuckRun._id.toString(),
            label: stuckRun.label,
          });
        }

        const stuckQueuedRuns = await Run.find({
          status: 'queued',
          lockedAt: { $lt: new Date(Date.now() - 10 * 60 * 1000) },
        });

        for (const stuckRun of stuckQueuedRuns) {
          await createNotification({
            type: 'run_stuck_queued',
            severity: 'warning',
            title: stuckRun.label + ' run stuck in queue',
            message: stuckRun.label + ' run (qty: ' + stuckRun.quantity + ') has been queued for over 10 minutes without execution.',
            schedulerOrderId: stuckRun.schedulerOrderId,
            runId: stuckRun._id.toString(),
            label: stuckRun.label,
          });
        }
      } catch (notifErr) {
        console.error('[SCHEDULER] Notification check error:', notifErr.message);
      }

      isSchedulerRunning = false;
    }
  }, 10000);
});
          

/* =========================
   HEALTH CHECK (for free-tier uptime monitors)
========================= */
app.get('/', (req, res) => {
  res.json({ ok: true, service: 'smm-scheduler', time: new Date().toISOString() });
});

/* =========================
   API ENDPOINTS
========================= */
app.post('/api/order', async (req, res) => {
  try {
    const { apiUrl, apiKey, link, services, name } = req.body;
    console.log("SERVICES RECEIVED:", JSON.stringify(services, null, 2));

    if (!apiUrl || !apiKey || !link || !services) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    console.log('Creating new order...');

    const schedulerOrderId = `sched-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

    // 🔥 FIX: Save Order FIRST, then runs
    // This prevents the scheduler from finding runs before the Order exists
    // (which would cause "Order not found" → run cancelled)
    const orderData = new Order({
      schedulerOrderId,
      name: name || `Order ${schedulerOrderId}`,
      link,
      status: 'pending',
      totalRuns: 0,
      completedRuns: 0,
      runStatuses: [],
      createdAt: new Date(),
      lastUpdatedAt: new Date(),
    });

    await orderData.save();
    console.log(`Order document created: ${schedulerOrderId}`);

    // 🔥 Now save runs — Order already exists so scheduler won't cancel them
    const runsForOrder = await addRuns(services, { apiUrl, apiKey, link }, schedulerOrderId);

    // 🔥 Update Order with correct run count
    await Order.updateOne(
      { schedulerOrderId },
      {
        $set: {
          totalRuns: runsForOrder.length,
          runStatuses: runsForOrder.map(() => 'pending'),
          lastUpdatedAt: new Date(),
        }
      }
    );

    console.log(`Order updated: ${schedulerOrderId} with ${runsForOrder.length} runs`);

    return res.json({
      success: true,
      message: 'Order scheduled (persistent)',
      schedulerOrderId,
      status: 'pending',
      completedRuns: 0,
      totalRuns: runsForOrder.length,
    });
  } catch (error) {
    console.error('[CREATE ORDER] Error:', error);
    return res.status(500).json({ error: error.message });
  }
});

app.post('/api/services', async (req, res) => {
  const { apiUrl, apiKey } = req.body;
  if (!apiUrl || !apiKey) {
    return res.status(400).json({ error: 'Missing API URL or key' });
  }
  try {
    const params = new URLSearchParams({ key: apiKey, action: 'services' });
    const response = await axios.post(apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    });
    return res.json(response.data);
  } catch (error) {
    return res.status(500).json({ error: error.response?.data || error.message });
  }
});

app.get('/api/order/status/:schedulerOrderId', async (req, res) => {
  try {
    const { schedulerOrderId } = req.params;
    const order = await Order.findOne({ schedulerOrderId });
    const orderRuns = await Run.find({ schedulerOrderId });

    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    return res.json({
      schedulerOrderId: order.schedulerOrderId,
      name: order.name,
      link: order.link,
      status: order.status,
      totalRuns: order.totalRuns,
      completedRuns: order.completedRuns,
      runStatuses: order.runStatuses,
      createdAt: order.createdAt,
      lastUpdatedAt: order.lastUpdatedAt,
      runs: orderRuns.map(r => ({
        id: r.id,
        label: r.label,
        quantity: r.quantity,
        time: r.time,
        status: r.status,
        smmOrderId: r.smmOrderId,
        executedAt: r.executedAt,
        error: r.error,
      })),
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/orders/status', async (req, res) => {
  try {
    const allOrders = await Order.find().sort({ createdAt: -1 });
    const ordersWithRuns = await Promise.all(allOrders.map(async (order) => {
      const orderRuns = await Run.find({ schedulerOrderId: order.schedulerOrderId });
      return {
        schedulerOrderId: order.schedulerOrderId,
        name: order.name,
        link: order.link,
        status: order.status,
        totalRuns: order.totalRuns,
        completedRuns: order.completedRuns,
        runStatuses: order.runStatuses,
        createdAt: order.createdAt,
        lastUpdatedAt: order.lastUpdatedAt,
        runs: orderRuns.map(r => ({
          id: r.id,
          label: r.label,
          quantity: r.quantity,
          time: r.time,
          status: r.status,
          smmOrderId: r.smmOrderId,
        })),
      };
    }));

    return res.json({ total: allOrders.length, orders: ordersWithRuns });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.post('/api/order/control', async (req, res) => {
  try {
    const { schedulerOrderId, action } = req.body;
    if (!schedulerOrderId || !action) {
      return res.status(400).json({ error: 'Missing schedulerOrderId or action' });
    }

    const order = await Order.findOne({ schedulerOrderId });
    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    if (action === 'cancel') {
      // 🔥 Bulk cancel all non-completed runs
            await Run.updateMany(
        {
          schedulerOrderId,
          status: { $in: ['pending', 'processing', 'queued', 'paused'] }
        },
        { $set: { status: 'cancelled', done: true, executionLock: null, claimedByTick: null } }
      );

      // 🔥 Remove from in-memory queues
      viewsQueue = viewsQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });
      likesQueue = likesQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });
      sharesQueue = sharesQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });
      savesQueue = savesQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });
           commentsQueue = commentsQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });
      repostsQueue = repostsQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });

                  // 🔥 NOTIFICATION: Order cancelled
      await createNotification({
        type: 'order_cancelled',
        severity: 'info',
        title: 'Order cancelled',
        message: 'Order ' + schedulerOrderId + ' was manually cancelled',
        schedulerOrderId: schedulerOrderId,
      });

      await Order.updateOne(
        { schedulerOrderId },
        { $set: { status: 'cancelled', lastUpdatedAt: new Date() } }
      );

      const updatedRuns = await Run.find({ schedulerOrderId });
      return res.json({
        success: true,
        status: 'cancelled',
        completedRuns: updatedRuns.filter(r => r.status === 'completed').length,
        runStatuses: updatedRuns.map(r => r.status),
      });
    }

    if (action === 'pause') {
      await Run.updateMany(
        {
          schedulerOrderId,
          status: { $in: ['pending', 'queued'] }
        },
        { $set: { status: 'paused', executionLock: null, claimedByTick: null } }
      );

      // Remove from queues
      viewsQueue = viewsQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });
      likesQueue = likesQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });
      sharesQueue = sharesQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });
      savesQueue = savesQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });
          commentsQueue = commentsQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });
      repostsQueue = repostsQueue.filter(item => {
        const run = item.run || item;
        return run.schedulerOrderId !== schedulerOrderId;
      });

      await Order.updateOne(
        { schedulerOrderId },
        { $set: { status: 'paused', lastUpdatedAt: new Date() } }
      );

      const updatedRuns = await Run.find({ schedulerOrderId });
      return res.json({
        success: true,
        status: 'paused',
        completedRuns: updatedRuns.filter(r => r.status === 'completed').length,
        runStatuses: updatedRuns.map(r => r.status),
      });
    }

    if (action === 'resume') {
      await Run.updateMany(
        {
          schedulerOrderId,
          status: 'paused'
        },
        { $set: { status: 'pending', executionLock: null, claimedByTick: null } }
      );

      await Order.updateOne(
        { schedulerOrderId },
        { $set: { status: 'running', lastUpdatedAt: new Date() } }
      );

      const updatedRuns = await Run.find({ schedulerOrderId });
      return res.json({
        success: true,
        status: 'running',
        completedRuns: updatedRuns.filter(r => r.status === 'completed').length,
        runStatuses: updatedRuns.map(r => r.status),
      });
    }

    return res.status(400).json({ error: 'Invalid action' });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/order/runs/:schedulerOrderId', async (req, res) => {
  try {
    const { schedulerOrderId } = req.params;
    const orderRuns = await Run.find({ schedulerOrderId });
    return res.json({
      schedulerOrderId,
      runs: orderRuns.map(r => ({
        id: r.id,
        label: r.label,
        quantity: r.quantity,
        time: r.time,
        status: r.status,
        smmOrderId: r.smmOrderId,
        executedAt: r.executedAt,
        error: r.error,
                done: r.done || false,
        cancelled: r.status === 'cancelled',
        lastError: r.error || "",
        retryCount: r.retryCount || 0,
        originalTime: r.originalScheduledTime ? r.originalScheduledTime.toISOString() : (r.time ? r.time.toISOString() : ""),
        currentTime: r.time ? r.time.toISOString() : "",
        actualExecutedAt: r.actualExecutedAt ? r.actualExecutedAt.toISOString() : null,
        retryReason: r.retryCount > 0 ? `Retried ${r.retryCount} time(s) due to active order conflict` : "",
      })),
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/settings/min-views', (req, res) => {
  return res.json({ minViewsPerRun: MIN_VIEWS_PER_RUN });
});

app.post('/api/settings/min-views', (req, res) => {
  const { minViewsPerRun } = req.body;
  if (typeof minViewsPerRun !== 'number' || minViewsPerRun < 1) {
    return res.status(400).json({ error: 'Invalid minViewsPerRun value' });
  }
  MIN_VIEWS_PER_RUN = Math.floor(minViewsPerRun);
  console.log(`Minimum views per run updated to: ${MIN_VIEWS_PER_RUN}`);
  return res.json({ success: true, minViewsPerRun: MIN_VIEWS_PER_RUN });
});

app.get('/api/queues/status', (req, res) => {
  return res.json({
    views: {
      queueLength: viewsQueue.length,
      isExecuting: isExecutingViews,
    },
    likes: {
      queueLength: likesQueue.length,
      isExecuting: isExecutingLikes,
    },
    shares: {
      queueLength: sharesQueue.length,
      isExecuting: isExecutingShares,
    },
    saves: {
      queueLength: savesQueue.length,
      isExecuting: isExecutingSaves,
    },
       comments: {
      queueLength: commentsQueue.length,
      isExecuting: isExecutingComments,
    },
    reposts: {
      queueLength: repostsQueue.length,
      isExecuting: isExecutingReposts,
    },
    scheduler: {
      isRunning: isSchedulerRunning,
      lastTickId: schedulerTickId,
    }
  });
});

app.post('/api/runs/retry-stuck', async (req, res) => {
  try {
    // Reset stuck processing runs (no executedAt = never actually executed)
    const stuckProcessing = await Run.updateMany(
      { status: 'processing', executedAt: null, lockedAt: { $lt: new Date(Date.now() - 120000) } },
      { $set: { status: 'pending', executionLock: null, lockedAt: null, claimedByTick: null } }
    );

    // Reset orphaned queued runs (stuck for over 2 minutes)
    const stuckQueued = await Run.updateMany(
      { status: 'queued', lockedAt: { $lt: new Date(Date.now() - 120000) } },
      { $set: { status: 'pending', executionLock: null, lockedAt: null, claimedByTick: null } }
    );

    // 🔥 Clear in-memory queues too
    viewsQueue = [];
    likesQueue = [];
    sharesQueue = [];
    savesQueue = [];
    commentsQueue = [];

    return res.json({
      success: true,
      resetProcessing: stuckProcessing.modifiedCount,
      resetQueued: stuckQueued.modifiedCount,
      message: `Reset ${stuckProcessing.modifiedCount + stuckQueued.modifiedCount} stuck runs`,
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.post('/api/scheduler/trigger', async (req, res) => {
  try {
    if (isSchedulerRunning) {
      return res.json({
        success: false,
        message: 'Scheduler is already running, try again in a few seconds',
      });
    }

    // 🔥 Manually trigger a scheduler tick
    const now = new Date();
    const tickId = `manual-${Date.now()}`;
    let addedToQueue = { views: 0, likes: 0, shares: 0, saves: 0, comments: 0 };

    let claimedCount = 0;
    while (claimedCount < 50) {
      const claimedRun = await Run.findOneAndUpdate(
        {
          status: 'pending',
          time: { $lte: now },
          executionLock: null,
          claimedByTick: null,
        },
        {
          $set: {
            status: 'queued',
            claimedByTick: tickId,
            executionLock: `claim-${tickId}`,
            lockedAt: new Date(),
          },
        },
        { new: true, sort: { time: 1 } }
      );

      if (!claimedRun) break;

      const queueItem = { run: claimedRun, tickId };
      const label = claimedRun.label;

      if (label === 'VIEWS') { viewsQueue.push(queueItem); addedToQueue.views++; }
      else if (label === 'LIKES') { likesQueue.push(queueItem); addedToQueue.likes++; }
      else if (label === 'SHARES') { sharesQueue.push(queueItem); addedToQueue.shares++; }
      else if (label === 'SAVES') { savesQueue.push(queueItem); addedToQueue.saves++; }
      else if (label === 'COMMENTS') { commentsQueue.push(queueItem); addedToQueue.comments++; }
      else if (label === 'REPOSTS') { repostsQueue.push(queueItem); addedToQueue.reposts++; }

      claimedCount++;
    }

    if (viewsQueue.length > 0 && !isExecutingViews) processViewsQueue();
    if (likesQueue.length > 0 && !isExecutingLikes) processLikesQueue();
    if (sharesQueue.length > 0 && !isExecutingShares) processSharesQueue();
    if (savesQueue.length > 0 && !isExecutingSaves) processSavesQueue();
    if (commentsQueue.length > 0 && !isExecutingComments) processCommentsQueue();

    return res.json({
      success: true,
      tickId,
      addedToQueue,
      currentQueues: {
        views: viewsQueue.length,
        likes: likesQueue.length,
        shares: sharesQueue.length,
        saves: savesQueue.length,
        comments: commentsQueue.length,
      }
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/* =========================
   START SERVER
========================= */
setInterval(async () => {
  try {
    const keepAliveUrl = process.env.KEEP_ALIVE_URL || "https://backend-new-6tzb.onrender.com/";
    await axios.get(keepAliveUrl, { timeout: 15000 });
    console.log("[PING] Keep-alive OK");
  } catch (e) {
    console.log("[PING] Keep-alive skipped/failed");
  }
}, 5 * 60 * 1000);

/* =========================
   🔥 NEW: Check provider order status
========================= */
app.post('/api/provider/check-status', async (req, res) => {
  try {
    const { schedulerOrderId } = req.body;
    if (!schedulerOrderId) {
      return res.status(400).json({ error: 'Missing schedulerOrderId' });
    }

    // Get all completed runs for this order that have a smmOrderId
    const runs = await Run.find({
      schedulerOrderId,
      status: 'completed',
      smmOrderId: { $ne: null },
    });

    if (runs.length === 0) {
      return res.json({ schedulerOrderId, results: [], message: 'No completed runs to check' });
    }

    // Group runs by API (apiUrl + apiKey) to batch check per provider
    const apiGroups = new Map();
    runs.forEach(run => {
      const key = `${run.apiUrl}|||${run.apiKey}`;
      if (!apiGroups.has(key)) {
        apiGroups.set(key, { apiUrl: run.apiUrl, apiKey: run.apiKey, runs: [] });
      }
      apiGroups.get(key).runs.push(run);
    });

    const results = [];

    for (const [, group] of apiGroups) {
      // Build comma-separated order IDs for batch check
      const orderIds = group.runs.map(r => r.smmOrderId).join(',');

      try {
        const params = new URLSearchParams({
          key: group.apiKey,
          action: 'status',
          orders: orderIds,
        });

        const response = await axios.post(group.apiUrl, params.toString(), {
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          timeout: 15000,
        });

        const providerData = response.data;

        // Process each run's provider status
        for (const run of group.runs) {
          const orderStatus = providerData[String(run.smmOrderId)];

          if (!orderStatus || orderStatus.error) {
            results.push({
              runId: run._id,
              smmOrderId: run.smmOrderId,
              label: run.label,
              providerStatus: 'unknown',
              error: orderStatus?.error || 'Not found in response',
            });
            continue;
          }

          const providerStatus = orderStatus.status || 'unknown';
          const remains = parseInt(orderStatus.remains || '0', 10);
          const startCount = parseInt(orderStatus.start_count || '0', 10);

          results.push({
            runId: run._id,
            smmOrderId: run.smmOrderId,
            label: run.label,
            providerStatus,
            remains,
            startCount,
            charge: orderStatus.charge,
            currency: orderStatus.currency,
          });

          // 🔥 If provider cancelled/partial, update run in DB
          if (providerStatus === 'Cancelled') {
            await Run.updateOne(
              { _id: run._id },
              { $set: { error: 'Provider cancelled this order' } }
            );
          } else if (providerStatus === 'Partial') {
            await Run.updateOne(
              { _id: run._id },
              { $set: { error: `Partial: ${remains} remaining out of ${startCount + remains}` } }
            );
          }
        }
      } catch (apiError) {
        console.error('[Provider Status Check] API error:', apiError.message);
        for (const run of group.runs) {
          results.push({
            runId: run._id,
            smmOrderId: run.smmOrderId,
            label: run.label,
            providerStatus: 'error',
            error: apiError.message,
          });
        }
      }
    }

    return res.json({ schedulerOrderId, total: results.length, results });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/* =========================
   🔥 NOTIFICATION API ENDPOINTS
========================= */
app.get('/api/notifications', async (req, res) => {
  try {
        const limit = parseInt(req.query.limit) || 50;
    const unreadOnly = req.query.unread === 'true';

    const filter = {};
    if (unreadOnly) filter.read = false;

    const notifications = await Notification.find(filter)
      .sort({ createdAt: -1 })
      .limit(limit);

    const unreadCount = await Notification.countDocuments({ read: false });

    return res.json({
      total: notifications.length,
      unreadCount,
      notifications,
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.post('/api/notifications/mark-read', async (req, res) => {
  try {
    const { notificationId, markAll } = req.body;

    if (markAll) {
      await Notification.updateMany({ read: false }, { $set: { read: true } });
      return res.json({ success: true, message: 'All notifications marked as read' });
    }

    if (notificationId) {
      await Notification.updateOne({ _id: notificationId }, { $set: { read: true } });
      return res.json({ success: true });
    }

    return res.status(400).json({ error: 'Missing notificationId or markAll' });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.delete('/api/notifications/clear', async (req, res) => {
  try {
    const result = await Notification.deleteMany({});
    return res.json({ success: true, deleted: result.deletedCount });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/* =========================
   🔥 DUPLICATE DETECTION ENDPOINT
========================= */
app.get('/api/check-duplicates', async (req, res) => {
  try {
    // Get all orders
    const allOrders = await Order.find({ status: { $ne: 'cancelled' } });
    const duplicates = [];

    for (const order of allOrders) {
      const runs = await Run.find({ schedulerOrderId: order.schedulerOrderId });

      // Count completed runs per label
      const completedCounts = {};
      const totalCounts = {};

      runs.forEach(r => {
        const label = r.label;
        if (!totalCounts[label]) totalCounts[label] = 0;
        totalCounts[label]++;

        if (r.status === 'completed' && r.smmOrderId) {
          if (!completedCounts[label]) completedCounts[label] = 0;
          completedCounts[label]++;
        }
      });

      // Check for duplicates: completed count should never exceed total count
      // Also check if multiple completed runs have different smmOrderIds for same time slot
      const smmOrdersByLabel = {};
      runs.forEach(r => {
        if (r.status === 'completed' && r.smmOrderId) {
          const label = r.label;
          if (!smmOrdersByLabel[label]) smmOrdersByLabel[label] = [];
          smmOrdersByLabel[label].push({
            smmOrderId: r.smmOrderId,
            quantity: r.quantity,
            executedAt: r.executedAt,
            time: r.time,
          });
        }
      });

      // Detect: same label has more completed runs than expected
      for (const label of Object.keys(completedCounts)) {
        const completed = completedCounts[label];
        const total = totalCounts[label];

        // If completed > total, something is wrong
        if (completed > total) {
          duplicates.push({
            schedulerOrderId: order.schedulerOrderId,
            orderName: order.name,
            link: order.link,
            label: label,
            expectedRuns: total,
            actualCompleted: completed,
            type: 'over_completed',
            smmOrders: smmOrdersByLabel[label] || [],
          });
        }
      }

      // Also detect: same smmOrderId used twice (exact duplicate placement)
      for (const label of Object.keys(smmOrdersByLabel)) {
        const orders_list = smmOrdersByLabel[label];
        const smmIds = orders_list.map(o => o.smmOrderId);
        const uniqueSmmIds = [...new Set(smmIds)];

        if (uniqueSmmIds.length < smmIds.length) {
          // Found duplicate smmOrderId
          const duplicatedIds = smmIds.filter((id, i) => smmIds.indexOf(id) !== i);
          duplicates.push({
            schedulerOrderId: order.schedulerOrderId,
            orderName: order.name,
            link: order.link,
            label: label,
            type: 'duplicate_smm_id',
            duplicatedSmmOrderIds: [...new Set(duplicatedIds)],
            allSmmOrders: orders_list,
          });
        }
      }

      // Detect: two completed runs of same label within 2 minutes of each other (suspicious)
      for (const label of Object.keys(smmOrdersByLabel)) {
        const sortedRuns = smmOrdersByLabel[label]
          .filter(r => r.executedAt)
          .sort((a, b) => new Date(a.executedAt).getTime() - new Date(b.executedAt).getTime());

        for (let i = 1; i < sortedRuns.length; i++) {
          const prev = new Date(sortedRuns[i - 1].executedAt).getTime();
          const curr = new Date(sortedRuns[i].executedAt).getTime();
          const diffMin = (curr - prev) / 60000;

          if (diffMin < 2 && sortedRuns[i].smmOrderId !== sortedRuns[i - 1].smmOrderId) {
            duplicates.push({
              schedulerOrderId: order.schedulerOrderId,
              orderName: order.name,
              link: order.link,
              label: label,
              type: 'rapid_double_execution',
              timeBetweenMin: Math.round(diffMin * 10) / 10,
              run1: sortedRuns[i - 1],
              run2: sortedRuns[i],
            });
          }
        }
      }
    }

    return res.json({
      scannedOrders: allOrders.length,
      duplicatesFound: duplicates.length,
      hasDuplicates: duplicates.length > 0,
      duplicates: duplicates,
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});
/* =========================
   🔥 MONGODB STORAGE CHECK
========================= */
app.get('/api/db-stats', async (req, res) => {
  try {
    const db = mongoose.connection.db;

    // Get database stats
    const dbStats = await db.stats();

    const dataStorageMB = Math.round((dbStats.dataSize / 1024 / 1024) * 100) / 100;
    const storageOnDiskMB = Math.round((dbStats.storageSize / 1024 / 1024) * 100) / 100;
    const indexSizeMB = Math.round((dbStats.indexSize / 1024 / 1024) * 100) / 100;
    const totalUsedMB = Math.round((dataStorageMB + indexSizeMB) * 100) / 100;
    const totalLimitMB = 512;
    const usagePercent = Math.round((totalUsedMB / totalLimitMB) * 100);

    // Get collection counts
    const runCount = await mongoose.connection.db.collection('runs').countDocuments();
    const orderCount = await mongoose.connection.db.collection('orders').countDocuments();
    const notifCount = await mongoose.connection.db.collection('notifications').countDocuments();

    return res.json({
      dataStorageMB,
      storageOnDiskMB,
      indexSizeMB,
      totalUsedMB,
      totalLimitMB,
      usagePercent,
      status: usagePercent > 85 ? 'critical' : usagePercent > 65 ? 'warning' : 'healthy',
      collections: {
        runs: runCount,
        orders: orderCount,
        notifications: notifCount,
      }
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});
/* =========================
   🔥 MEMORY USAGE ENDPOINT
========================= */
app.get('/api/memory-usage', (req, res) => {
  const mem = process.memoryUsage();
  const usedMB = Math.round(mem.rss / 1024 / 1024);
  const heapUsedMB = Math.round(mem.heapUsed / 1024 / 1024);
  const heapTotalMB = Math.round(mem.heapTotal / 1024 / 1024);
  const totalMB = 512; // Render free tier limit
  const usagePercent = Math.round((usedMB / totalMB) * 100);

  return res.json({
    usedMB,
    totalMB,
    usagePercent,
    heapUsedMB,
    heapTotalMB,
    status: usagePercent > 85 ? 'critical' : usagePercent > 65 ? 'warning' : 'healthy',
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`========================================`);
  console.log(`Server running on port ${PORT}`);
  console.log(`Minimum views per run: ${MIN_VIEWS_PER_RUN}`);
  console.log(`6 Queue system: VIEWS | LIKES | SHARES | SAVES | COMMENTS | REPOSTS`);
  console.log(`Scheduler runs every 10 seconds`);
  console.log(`🔒 Bulletproof atomic execution locks ENABLED`);
  console.log(`🔒 Scheduler mutex ENABLED`);
  console.log(`🔒 Tick-based claim tracking ENABLED`);
  console.log(`========================================`);
});
