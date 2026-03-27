const express = require('express');
const cors = require('cors');
const axios = require('axios');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

const DATA_FILE = 'runs.json';
const LINK_TRACKER_FILE = 'link_tracker.json';

// 🔥 Configuration
const RETRY_DELAY_MINUTES = 5;
const MAX_RETRY_HOURS = 4;
const STATUS_CACHE_MINUTES = 2;

// 🔥 Active SMM orders tracker per link
let linkTracker = {};

// Status values that mean "order is still active"
const ACTIVE_ORDER_STATUSES = [
  'pending',
  'in progress',
  'processing',
  'partial',
  'inprogress',
];

/* =========================
   LOAD + SAVE
========================= */
function loadRuns() {
  if (!fs.existsSync(DATA_FILE)) return [];
  try {
    const data = JSON.parse(fs.readFileSync(DATA_FILE));
    return data.map(run => ({
      ...run,
      isExecuting: false
    }));
  } catch (err) {
    console.error('[Load Runs] Error:', err.message);
    return [];
  }
}

function saveRuns(runs) {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(runs, null, 2));
  } catch (err) {
    console.error('[Save Runs] Error:', err.message);
  }
}

function loadLinkTracker() {
  if (!fs.existsSync(LINK_TRACKER_FILE)) return {};
  try {
    return JSON.parse(fs.readFileSync(LINK_TRACKER_FILE));
  } catch (err) {
    console.error('[Load Link Tracker] Error:', err.message);
    return {};
  }
}

function saveLinkTracker(tracker) {
  try {
    fs.writeFileSync(LINK_TRACKER_FILE, JSON.stringify(tracker, null, 2));
  } catch (err) {
    console.error('[Save Link Tracker] Error:', err.message);
  }
}

let allRuns = loadRuns();
linkTracker = loadLinkTracker();

console.log(`[Startup] Loaded ${allRuns.length} runs`);
console.log(`[Startup] Tracking ${Object.keys(linkTracker).length} active links`);

const executingRunIds = new Set();

// 🔥 NEW: Track which links are currently being processed (in this scheduler cycle)
const linksBeingProcessed = new Set();

/* =========================
   CHECK ORDER STATUS (SMM PANEL API)
========================= */
async function checkOrderStatus(apiUrl, apiKey, smmOrderId) {
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

    const data = response.data;
    
    if (data && data.status) {
      return {
        status: String(data.status).toLowerCase(),
        remains: data.remains || 0,
        charge: data.charge || 0,
      };
    }

    return null;
  } catch (err) {
    console.error(`[Check Status] Error for order ${smmOrderId}:`, err.message);
    return null;
  }
}

/* =========================
   CHECK IF LINK HAS ACTIVE ORDER
========================= */
async function hasActiveSmmOrder(link, apiUrl, apiKey) {
  const tracker = linkTracker[link];
  
  if (!tracker || !tracker.smmOrderId) {
    return false;
  }

  // Check cache age
  const cacheAge = Date.now() - new Date(tracker.lastChecked).getTime();
  const cacheExpired = cacheAge > (STATUS_CACHE_MINUTES * 60 * 1000);

  // If cache is fresh and status was active, assume still active
  if (!cacheExpired && tracker.isActive) {
    console.log(`[Link Check] ${link.slice(-30)} - Using cached status (active)`);
    return true;
  }

  // Cache expired or was inactive - check again
  console.log(`[Link Check] ${link.slice(-30)} - Checking SMM order ${tracker.smmOrderId}...`);
  
  const statusResult = await checkOrderStatus(apiUrl, apiKey, tracker.smmOrderId);

  if (!statusResult) {
    // API failed - assume inactive to avoid blocking
    console.warn(`[Link Check] ${link.slice(-30)} - Status check failed, assuming inactive`);
    tracker.isActive = false;
    tracker.lastChecked = new Date().toISOString();
    saveLinkTracker(linkTracker);
    return false;
  }

  const isActive = ACTIVE_ORDER_STATUSES.includes(statusResult.status);

  // Update tracker
  tracker.status = statusResult.status;
  tracker.isActive = isActive;
  tracker.lastChecked = new Date().toISOString();
  tracker.remains = statusResult.remains;

  console.log(`[Link Check] ${link.slice(-30)} - Status: ${statusResult.status}, Active: ${isActive}, Remains: ${statusResult.remains}`);

  // If order completed, remove from tracker
  if (!isActive) {
    console.log(`[Link Check] ${link.slice(-30)} - Order completed, removing from tracker`);
    delete linkTracker[link];
  }

  saveLinkTracker(linkTracker);

  return isActive;
}

/* =========================
   REGISTER SMM ORDER FOR LINK
========================= */
function registerSmmOrder(link, smmOrderId, apiUrl) {
  linkTracker[link] = {
    smmOrderId,
    apiUrl,
    createdAt: new Date().toISOString(),
    lastChecked: new Date().toISOString(),
    isActive: true,
    status: 'pending',
    remains: 0,
  };
  saveLinkTracker(linkTracker);
  console.log(`[Link Tracker] Registered order ${smmOrderId} for ${link.slice(-30)}`);
}

/* =========================
   CHECK IF RUN TIMED OUT
========================= */
function isRunTimedOut(run) {
  if (!run.originalTime) return false;
  const originalTime = new Date(run.originalTime).getTime();
  const maxTime = originalTime + (MAX_RETRY_HOURS * 60 * 60 * 1000);
  return Date.now() > maxTime;
}

/* =========================
   PLACE ORDER (SMM PANEL API)
========================= */
async function placeOrder({ apiUrl, apiKey, service, link, quantity }) {
  const params = new URLSearchParams({
    key: apiKey,
    action: 'add',
    service: String(service),
    link: String(link),
    quantity: String(quantity),
  });

  const response = await axios.post(apiUrl, params.toString(), {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    timeout: 30000,
  });

  return response.data;
}

/* =========================
   GENERATE SCHEDULER ORDER ID
========================= */
function generateSchedulerOrderId() {
  return `SCH-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

/* =========================
   ADD RUNS TO STORAGE
========================= */
function addRuns(services, baseConfig, schedulerOrderId) {
  Object.entries(services).forEach(([key, serviceConfig]) => {
    if (!serviceConfig) return;

    const label = key.toUpperCase();

    serviceConfig.runs.forEach((run, index) => {
      const runTime = run.time;
      allRuns.push({
        id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        schedulerOrderId,
        label,
        apiUrl: baseConfig.apiUrl,
        apiKey: baseConfig.apiKey,
        service: serviceConfig.serviceId,
        link: baseConfig.link,
        quantity: run.quantity,
        originalTime: runTime,
        time: runTime,
        runIndex: index + 1, // 🔥 NEW: Track run order (1, 2, 3, 4...)
        done: false,
        cancelled: false,
        paused: false,
        isExecuting: false,
        retryCount: 0,
        retryReason: null,
        lastError: null,
        executedAt: null,
        smmOrderId: null,
        createdAt: new Date().toISOString(),
      });
    });
  });

  saveRuns(allRuns);
}

/* =========================
   HANDLE RETRY / RESCHEDULE
========================= */
function handleRunRetry(run, errorMessage, reason = 'API Error') {
  const isTimedOut = isRunTimedOut(run);

  run.lastError = errorMessage;

  if (isTimedOut) {
    console.log(`[${run.label}] ⏰ TIMEOUT - Exceeded ${MAX_RETRY_HOURS} hours`);
    run.done = true;
    run.retryReason = `Timeout: Exceeded ${MAX_RETRY_HOURS}h retry limit`;
    return;
  }

  run.retryCount++;
  run.retryReason = reason;

  // Reschedule for RETRY_DELAY_MINUTES later
  const newTime = new Date(Date.now() + RETRY_DELAY_MINUTES * 60 * 1000).toISOString();
  run.time = newTime;

  console.log(`[${run.label}] 🔄 ${reason} - Retry #${run.retryCount} at ${newTime}`);
}

/* =========================
   🔥 GET NEXT PENDING RUN FOR A LINK (Ordered by originalTime)
========================= */
function getNextPendingRunForLink(link) {
  const now = Date.now();
  
  // Get all pending runs for this link
  const pendingRunsForLink = allRuns.filter(run => 
    run.link === link &&
    !run.done &&
    !run.cancelled &&
    !run.paused &&
    !run.isExecuting &&
    !executingRunIds.has(run.id) &&
    new Date(run.time).getTime() <= now
  );

  if (pendingRunsForLink.length === 0) {
    return null;
  }

  // 🔥 Sort by ORIGINAL time (first scheduled = first executed)
  pendingRunsForLink.sort((a, b) => {
    const timeA = new Date(a.originalTime).getTime();
    const timeB = new Date(b.originalTime).getTime();
    
    // If same original time, sort by runIndex
    if (timeA === timeB) {
      return (a.runIndex || 0) - (b.runIndex || 0);
    }
    
    return timeA - timeB;
  });

  // Return the first one (lowest original time)
  return pendingRunsForLink[0];
}

/* =========================
   🔥 GET ALL LINKS WITH PENDING RUNS
========================= */
function getLinksWithPendingRuns() {
  const now = Date.now();
  const links = new Set();

  allRuns.forEach(run => {
    if (
      !run.done &&
      !run.cancelled &&
      !run.paused &&
      !run.isExecuting &&
      !executingRunIds.has(run.id) &&
      new Date(run.time).getTime() <= now
    ) {
      links.add(run.link);
    }
  });

  return Array.from(links);
}

/* =========================
   EXECUTE SINGLE RUN
========================= */
async function executeRun(run) {
  const runId = run.id;

  if (executingRunIds.has(runId)) {
    return;
  }

  if (run.cancelled) {
    run.done = true;
    return;
  }

  if (run.done || run.paused) {
    return;
  }

  if (isRunTimedOut(run)) {
    run.done = true;
    run.retryReason = `Timeout: Exceeded ${MAX_RETRY_HOURS}h retry limit`;
    console.log(`[${run.label}] ⏰ TIMEOUT before execution`);
    return;
  }

  // 🔥 CHECK IF LINK HAS ACTIVE ORDER
  const linkHasActiveOrder = await hasActiveSmmOrder(run.link, run.apiUrl, run.apiKey);

  if (linkHasActiveOrder) {
    console.log(`[${run.label}] 🔒 Link has active order - rescheduling Run #${run.runIndex || '?'}...`);
    handleRunRetry(
      run,
      'Link has active SMM order',
      'Waiting for previous order to complete'
    );
    saveRuns(allRuns);
    return;
  }

  // Mark as executing
  executingRunIds.add(runId);
  run.isExecuting = true;

  try {
    if (!run.quantity || run.quantity <= 0) {
      console.log(`[${run.label}] Skipping - invalid quantity`);
      run.done = true;
      return;
    }

    console.log(`[${run.label}] ▶️ Executing Run #${run.runIndex || '?'}: ${run.quantity} to ${run.link.slice(-30)} (Retry: ${run.retryCount})`);

    const result = await placeOrder(run);

    if (run.cancelled) {
      console.log(`[${run.label}] Cancelled during execution`);
      run.done = true;
      return;
    }

    if (result?.order) {
      // 🔥 SUCCESS - Register SMM order for this link
      const smmOrderId = String(result.order);
      console.log(`[${run.label}] ✅ SUCCESS Run #${run.runIndex || '?'} - SMM Order: ${smmOrderId}`);
      
      run.done = true;
      run.executedAt = new Date().toISOString();
      run.smmOrderId = smmOrderId;
      run.lastError = null;
      run.retryReason = null;

      // 🔥 Register this order for the link
      registerSmmOrder(run.link, smmOrderId, run.apiUrl);

    } else if (result?.error) {
      console.error(`[${run.label}] ❌ API Error:`, result.error);
      handleRunRetry(run, result.error, 'SMM Panel Error');
    } else {
      console.error(`[${run.label}] ❌ Unknown response:`, JSON.stringify(result));
      handleRunRetry(run, 'Unknown API response', 'Invalid Response');
    }

  } catch (err) {
    const errorMsg = err.response?.data?.error || err.response?.data?.message || err.message || 'Unknown error';
    console.error(`[${run.label}] ❌ EXCEPTION:`, errorMsg);
    handleRunRetry(run, errorMsg, 'Network/API Error');
  } finally {
    run.isExecuting = false;
    executingRunIds.delete(runId);
    saveRuns(allRuns);
  }
}

/* =========================
   🔥 MAIN SCHEDULER (FIXED - ORDERED EXECUTION)
========================= */
let isSchedulerRunning = false;

async function runScheduler() {
  if (isSchedulerRunning) {
    return;
  }

  isSchedulerRunning = true;

  try {
    // 🔥 Clean up cancelled runs
    allRuns.forEach(run => {
      if (run.cancelled && !run.done) {
        run.done = true;
      }
    });

    // 🔥 Get all unique links that have pending runs
    const linksWithPendingRuns = getLinksWithPendingRuns();

    if (linksWithPendingRuns.length === 0) {
      // No pending runs, skip
      isSchedulerRunning = false;
      return;
    }

    console.log(`[Scheduler] Found ${linksWithPendingRuns.length} links with pending runs`);

    // 🔥 Process ONE run per link (the one with lowest originalTime)
    const runsToExecute = [];

    for (const link of linksWithPendingRuns) {
      // Get the NEXT run for this link (ordered by originalTime)
      const nextRun = getNextPendingRunForLink(link);
      
      if (nextRun) {
        runsToExecute.push(nextRun);
        console.log(`[Scheduler] Queued Run #${nextRun.runIndex || '?'} for ${link.slice(-30)} (original: ${nextRun.originalTime})`);
      }
    }

    if (runsToExecute.length > 0) {
      console.log(`[Scheduler] Processing ${runsToExecute.length} runs (1 per link, ordered)...`);
      
      // Execute all runs in parallel (but only 1 per link!)
      const CONCURRENCY_LIMIT = 5;
      
      for (let i = 0; i < runsToExecute.length; i += CONCURRENCY_LIMIT) {
        const batch = runsToExecute.slice(i, i + CONCURRENCY_LIMIT);
        
        await Promise.all(batch.map(run => executeRun(run).catch(err => {
          console.error(`[Scheduler] Run execution error:`, err.message);
        })));
      }
    }

    saveRuns(allRuns);

    // Log stats
    const stats = {
      total: allRuns.length,
      pending: allRuns.filter(r => !r.done && !r.cancelled && !r.paused).length,
      retrying: allRuns.filter(r => !r.done && !r.cancelled && r.retryCount > 0).length,
      done: allRuns.filter(r => r.done && !r.cancelled).length,
      cancelled: allRuns.filter(r => r.cancelled).length,
      activeLinks: Object.keys(linkTracker).length,
    };
    
    if (runsToExecute.length > 0) {
      console.log(`[Scheduler] Stats:`, stats);
    }

  } catch (err) {
    console.error('[Scheduler] Error:', err.message);
  } finally {
    isSchedulerRunning = false;
  }
}

// Run scheduler every 10 seconds
setInterval(runScheduler, 10000);
setTimeout(runScheduler, 3000);

/* =========================
   CREATE ORDER
========================= */
app.post('/api/order', async (req, res) => {
  const { apiUrl, apiKey, link, services, name } = req.body;

  if (!apiUrl || !apiKey || !link || !services) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  const schedulerOrderId = generateSchedulerOrderId();

  console.log(`[Create Order] New order: ${schedulerOrderId}`);
  console.log(`[Create Order] Link: ${link}`);

  addRuns(services, { apiUrl, apiKey, link }, schedulerOrderId);

  const runsAdded = allRuns.filter(r => r.schedulerOrderId === schedulerOrderId).length;
  console.log(`[Create Order] ✅ Added ${runsAdded} runs`);

  return res.json({
    success: true,
    message: 'Order scheduled successfully',
    schedulerOrderId,
    runsAdded,
  });
});

/* =========================
   ORDER CONTROL
========================= */
app.post('/api/order/control', (req, res) => {
  const { schedulerOrderId, action } = req.body;

  console.log(`[Order Control] ${action?.toUpperCase()} for ${schedulerOrderId}`);

  if (!schedulerOrderId || !action) {
    return res.status(400).json({ 
      success: false,
      error: 'Missing schedulerOrderId or action' 
    });
  }

  if (!['pause', 'resume', 'cancel'].includes(action)) {
    return res.status(400).json({ 
      success: false,
      error: 'Invalid action' 
    });
  }

  const orderRuns = allRuns.filter(run => run.schedulerOrderId === schedulerOrderId);

  if (orderRuns.length === 0) {
    return res.status(404).json({ 
      success: false,
      error: 'Order not found' 
    });
  }

  let affectedCount = 0;

  orderRuns.forEach(run => {
    if (action === 'cancel') {
      if (!run.done || !run.cancelled) {
        run.cancelled = true;
        run.done = true;
        run.paused = false;
        run.isExecuting = false;
        executingRunIds.delete(run.id);
        affectedCount++;
      }
    } else if (action === 'pause') {
      if (!run.done && !run.cancelled) {
        run.paused = true;
        affectedCount++;
      }
    } else if (action === 'resume') {
      if (!run.done && !run.cancelled && run.paused) {
        run.paused = false;
        affectedCount++;
      }
    }
  });

  saveRuns(allRuns);

  const stats = {
    total: orderRuns.length,
    completed: orderRuns.filter(r => r.done && !r.cancelled).length,
    cancelled: orderRuns.filter(r => r.cancelled).length,
    paused: orderRuns.filter(r => r.paused && !r.done).length,
    pending: orderRuns.filter(r => !r.done && !r.cancelled && !r.paused).length,
    retrying: orderRuns.filter(r => !r.done && !r.cancelled && r.retryCount > 0).length,
  };

  let status = 'running';
  if (stats.cancelled === stats.total) status = 'cancelled';
  else if (stats.completed === stats.total) status = 'completed';
  else if (stats.paused > 0 && stats.pending === 0) status = 'paused';

  console.log(`[Order Control] ✅ ${action}: ${affectedCount} runs affected`, stats);

  return res.json({
    success: true,
    status,
    ...stats,
    affectedRuns: affectedCount,
    runStatuses: orderRuns.map(r => {
      if (r.cancelled) return 'cancelled';
      if (r.done) return 'completed';
      if (r.retryCount > 0) return 'retrying';
      return 'pending';
    }),
  });
});

/* =========================
   GET ORDER RUNS
========================= */
app.get('/api/order/runs/:schedulerOrderId', (req, res) => {
  const { schedulerOrderId } = req.params;

  const orderRuns = allRuns.filter(run => run.schedulerOrderId === schedulerOrderId);

  if (orderRuns.length === 0) {
    return res.status(404).json({ error: 'Order not found' });
  }

  // 🔥 Sort by runIndex for consistent ordering
  orderRuns.sort((a, b) => (a.runIndex || 0) - (b.runIndex || 0));

  return res.json({
    schedulerOrderId,
    runs: orderRuns.map(r => ({
      id: r.id,
      label: r.label,
      quantity: r.quantity,
      runIndex: r.runIndex,
      originalTime: r.originalTime,
      currentTime: r.time,
      done: r.done,
      cancelled: r.cancelled,
      paused: r.paused,
      isExecuting: r.isExecuting,
      retryCount: r.retryCount,
      retryReason: r.retryReason,
      lastError: r.lastError,
      executedAt: r.executedAt,
      smmOrderId: r.smmOrderId,
    })),
  });
});

/* =========================
   LEGACY CANCEL
========================= */
app.post('/api/cancel', (req, res) => {
  const { link } = req.body;

  if (!link) {
    return res.status(400).json({ error: 'Missing link' });
  }

  let cancelledCount = 0;

  allRuns.forEach(run => {
    if (run.link === link && !run.done) {
      run.cancelled = true;
      run.done = true;
      executingRunIds.delete(run.id);
      cancelledCount++;
    }
  });

  // Also remove from link tracker
  if (linkTracker[link]) {
    delete linkTracker[link];
    saveLinkTracker(linkTracker);
  }

  saveRuns(allRuns);

  return res.json({
    success: true,
    cancelledRuns: cancelledCount,
  });
});

/* =========================
   GET ORDER STATUS
========================= */
app.get('/api/order/status/:schedulerOrderId', (req, res) => {
  const { schedulerOrderId } = req.params;

  const orderRuns = allRuns.filter(run => run.schedulerOrderId === schedulerOrderId);

  if (orderRuns.length === 0) {
    return res.status(404).json({ error: 'Order not found' });
  }

  // 🔥 Sort by runIndex
  orderRuns.sort((a, b) => (a.runIndex || 0) - (b.runIndex || 0));

  const stats = {
    total: orderRuns.length,
    completed: orderRuns.filter(r => r.done && !r.cancelled).length,
    cancelled: orderRuns.filter(r => r.cancelled).length,
    paused: orderRuns.filter(r => r.paused).length,
    pending: orderRuns.filter(r => !r.done && !r.cancelled && !r.paused).length,
    retrying: orderRuns.filter(r => !r.done && !r.cancelled && r.retryCount > 0).length,
    executing: orderRuns.filter(r => r.isExecuting).length,
  };

  let status = 'running';
  if (stats.cancelled === stats.total) status = 'cancelled';
  else if (stats.completed === stats.total) status = 'completed';
  else if (stats.paused > 0 && stats.pending === 0) status = 'paused';

  return res.json({
    schedulerOrderId,
    status,
    ...stats,
    runs: orderRuns.map(r => ({
      id: r.id,
      label: r.label,
      quantity: r.quantity,
      runIndex: r.runIndex,
      originalTime: r.originalTime,
      currentTime: r.time,
      done: r.done,
      cancelled: r.cancelled,
      paused: r.paused,
      isExecuting: r.isExecuting,
      retryCount: r.retryCount,
      retryReason: r.retryReason,
      lastError: r.lastError,
      executedAt: r.executedAt,
    })),
  });
});

/* =========================
   DEBUG ENDPOINTS
========================= */
app.get('/api/debug/runs', (req, res) => {
  const stats = {
    total: allRuns.length,
    pending: allRuns.filter(r => !r.done && !r.cancelled && !r.paused).length,
    retrying: allRuns.filter(r => !r.done && !r.cancelled && r.retryCount > 0).length,
    executing: allRuns.filter(r => r.isExecuting).length,
    done: allRuns.filter(r => r.done && !r.cancelled).length,
    cancelled: allRuns.filter(r => r.cancelled).length,
    paused: allRuns.filter(r => r.paused).length,
    withErrors: allRuns.filter(r => r.lastError).length,
  };

  const pendingRuns = allRuns
    .filter(r => !r.done && !r.cancelled && !r.paused)
    .sort((a, b) => new Date(a.originalTime).getTime() - new Date(b.originalTime).getTime())
    .slice(0, 30)
    .map(r => ({
      id: r.id,
      schedulerOrderId: r.schedulerOrderId,
      label: r.label,
      runIndex: r.runIndex,
      link: r.link.slice(-30),
      quantity: r.quantity,
      originalTime: r.originalTime,
      currentTime: r.time,
      isExecuting: r.isExecuting,
      retryCount: r.retryCount,
      retryReason: r.retryReason,
      lastError: r.lastError,
    }));

  return res.json({
    stats,
    pendingRuns,
    executingIds: Array.from(executingRunIds),
    activeLinks: Object.keys(linkTracker).map(link => ({
      link: link.slice(-40),
      ...linkTracker[link],
    })),
  });
});

app.get('/api/debug/link-tracker', (req, res) => {
  return res.json({
    activeLinks: Object.keys(linkTracker).length,
    links: Object.keys(linkTracker).map(link => ({
      link: link.slice(-50),
      ...linkTracker[link],
    })),
  });
});

app.post('/api/debug/retry-stuck', (req, res) => {
  let fixedCount = 0;

  allRuns.forEach(run => {
    if (run.isExecuting && !executingRunIds.has(run.id)) {
      run.isExecuting = false;
      fixedCount++;
    }

    const runTime = new Date(run.time).getTime();
    const now = Date.now();
    const twoHoursAgo = now - (2 * 60 * 60 * 1000);

    if (!run.done && !run.cancelled && !run.paused && runTime < twoHoursAgo && !isRunTimedOut(run)) {
      run.time = new Date().toISOString();
      run.isExecuting = false;
      fixedCount++;
      console.log(`[Debug] Rescheduled stuck run #${run.runIndex}: ${run.id}`);
    }
  });

  saveRuns(allRuns);

  return res.json({
    success: true,
    fixedRuns: fixedCount,
  });
});

// 🔥 NEW: Clear link tracker
app.post('/api/debug/clear-link-tracker', (req, res) => {
  const count = Object.keys(linkTracker).length;
  linkTracker = {};
  saveLinkTracker(linkTracker);
  
  console.log(`[Debug] Cleared ${count} links from tracker`);
  
  return res.json({
    success: true,
    clearedLinks: count,
  });
});

/* =========================
   FETCH SERVICES
========================= */
app.post('/api/services', async (req, res) => {
  const { apiUrl, apiKey } = req.body;

  if (!apiUrl || !apiKey) {
    return res.status(400).json({ error: 'Missing API URL or key' });
  }

  try {
    const params = new URLSearchParams({
      key: apiKey,
      action: 'services',
    });

    const response = await axios.post(apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 30000,
    });

    return res.json(response.data);
  } catch (error) {
    return res.status(500).json({
      error: error.response?.data || error.message,
    });
  }
});

/* =========================
   HEALTH CHECK
========================= */
app.get('/api/health', (req, res) => {
  return res.json({
    status: 'ok',
    uptime: process.uptime(),
    totalRuns: allRuns.length,
    pendingRuns: allRuns.filter(r => !r.done && !r.cancelled && !r.paused).length,
    retryingRuns: allRuns.filter(r => !r.done && !r.cancelled && r.retryCount > 0).length,
    executingNow: executingRunIds.size,
    activeLinks: Object.keys(linkTracker).length,
  });
});

/* =========================
   START SERVER
========================= */
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📊 Loaded ${allRuns.length} existing runs`);
  
  const pending = allRuns.filter(r => !r.done && !r.cancelled && !r.paused).length;
  const retrying = allRuns.filter(r => !r.done && r.retryCount > 0).length;
  console.log(`⏳ ${pending} runs pending, ${retrying} retrying`);
  console.log(`🔗 ${Object.keys(linkTracker).length} active links tracked`);
});

/* =========================
   KEEP SERVER ALIVE
========================= */
setInterval(async () => {
  try {
    await axios.get("https://backend-new-6tzb.onrender.com/api/health");
    console.log("[Keep Alive] ✅");
  } catch (e) {}
}, 5 * 60 * 1000);
