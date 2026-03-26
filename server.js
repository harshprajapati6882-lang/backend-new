const express = require('express');
const cors = require('cors');
const axios = require('axios');
const fs = require('fs');
const { randomUUID } = require('crypto');

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

const DATA_FILE = 'runs.json';

/* =========================
   LOAD + SAVE RUNS
========================= */
function loadRuns() {
  if (!fs.existsSync(DATA_FILE)) return [];
  try {
    return JSON.parse(fs.readFileSync(DATA_FILE));
  } catch (e) {
    return [];
  }
}

function saveRuns(runs) {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(runs, null, 2));
  } catch (e) {
    console.error('[SAVE] Failed to save runs.json:', e.message);
  }
}

let allRuns = loadRuns();

/* =========================
   PLACE ORDER
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
    timeout: 15000,
  });

  return response.data;
}

/* =========================
   CANCEL ORDER IN SMM PANEL
========================= */
async function cancelOrderInSmmPanel({ apiUrl, apiKey, orderId }) {
  try {
    const params = new URLSearchParams({
      key: apiKey,
      action: 'cancel',
      order: String(orderId),
    });

    const response = await axios.post(apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 15000,
    });

    console.log(`[CANCEL] SMM Panel response for order ${orderId}:`, response.data);
    return response.data;
  } catch (err) {
    console.error(`[CANCEL] Failed to cancel order ${orderId} in SMM panel:`, err.response?.data || err.message);
    return { error: err.message };
  }
}

/* =========================
   CHECK ORDER STATUS FROM SMM PANEL
========================= */
async function checkOrderStatus({ apiUrl, apiKey, orderId }) {
  const params = new URLSearchParams({
    key: apiKey,
    action: 'status',
    order: String(orderId),
  });

  try {
    const response = await axios.post(apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 15000,
    });
    return response.data;
  } catch (err) {
    console.error('Status check failed:', err.message);
    return null;
  }
}

/* =========================
   ADD RUNS TO STORAGE
   FIX: Accept schedulerOrderId, startDelayHours, name
        Use randomUUID() for unique IDs
        Apply startDelayHours offset to every run time
========================= */
function addRuns(services, baseConfig) {
  const delayMs = (baseConfig.startDelayHours || 0) * 3600 * 1000;

  Object.entries(services).forEach(([key, serviceConfig]) => {
    if (!serviceConfig) return;

    const label = key.toUpperCase();

    serviceConfig.runs.forEach((run) => {
      // Apply start delay offset to the scheduled run time
      const originalTime = new Date(run.time).getTime();
      const scheduledTime = new Date(originalTime + delayMs).toISOString();

      allRuns.push({
        id: randomUUID(),                              // FIX: unique ID, no collisions
        schedulerOrderId: baseConfig.schedulerOrderId, // FIX: link run to parent order
        label,
        name: baseConfig.name || '',
        apiUrl: baseConfig.apiUrl,
        apiKey: baseConfig.apiKey,
        service: serviceConfig.serviceId,
        link: baseConfig.link,
        quantity: run.quantity,
        time: scheduledTime,                           // FIX: start delay applied
        done: false,
        cancelled: false,
        paused: false,
        retryCount: 0,
        isExecuting: false,
        smmOrderId: null,
        smmStatus: 'pending',
        createdAt: new Date().toISOString(),
      });
    });
  });

  saveRuns(allRuns); // FIX: save immediately after adding
}

/* =========================
   EXECUTE RUN (SAFE + RETRY)
========================= */
async function executeRun(run) {
  if (run.cancelled || run.done || run.isExecuting || run.paused) return;

  run.isExecuting = true;

  try {
    if (!run.quantity || run.quantity <= 0) {
      run.done = true;
      run.isExecuting = false;
      saveRuns(allRuns);
      return;
    }

    console.log(`[${run.label}] Executing run ${run.id} for link: ${run.link}`);

    const result = await placeOrder(run);

    if (run.cancelled) {
      run.isExecuting = false;
      saveRuns(allRuns);
      return;
    }

    if (result?.order) {
      console.log(`[${run.label}] SUCCESS orderId=${result.order}`);
      run.smmOrderId = result.order;
      run.smmStatus = 'processing';
      saveRuns(allRuns); // FIX: save immediately on success
    } else {
      console.error(`[${run.label}] FAILED`, result);

      if (run.retryCount < 3 && !run.cancelled) {
        run.retryCount++;
        console.log(`[${run.label}] Retrying in 60s... attempt ${run.retryCount}`);
        run.isExecuting = false;
        saveRuns(allRuns);
        setTimeout(() => executeRun(run), 60000);
        return;
      } else {
        console.error(`[${run.label}] Max retries reached`);
        run.done = true;
        run.smmStatus = 'failed';
        saveRuns(allRuns);
      }
    }

  } catch (err) {
    console.error(`[${run.label}] ERROR`, err.response?.data || err.message);

    if (run.retryCount < 3 && !run.cancelled) {
      run.retryCount++;
      console.log(`[${run.label}] Retrying after error... attempt ${run.retryCount}`);
      run.isExecuting = false;
      saveRuns(allRuns);
      setTimeout(() => executeRun(run), 60000);
      return;
    } else {
      console.error(`[${run.label}] Max retries reached after error`);
      run.done = true;
      run.smmStatus = 'failed';
      saveRuns(allRuns);
    }
  }

  run.isExecuting = false;
}

/* =========================
   CHECK RUN STATUSES FROM SMM PANEL
========================= */
async function checkRunStatuses() {
  let changed = false;

  for (let run of allRuns) {
    if (run.done || run.cancelled || !run.smmOrderId) continue;

    const statusData = await checkOrderStatus({
      apiUrl: run.apiUrl,
      apiKey: run.apiKey,
      orderId: run.smmOrderId,
    });

    if (statusData && statusData.status) {
      const smmStatus = statusData.status.toLowerCase();
      run.smmStatus = smmStatus;
      changed = true;

      if (smmStatus === 'completed' || smmStatus === 'complete') {
        run.done = true;
        console.log(`[${run.label}] Order ${run.smmOrderId} completed`);
      } else if (smmStatus === 'canceled' || smmStatus === 'cancelled' || smmStatus === 'refunded') {
        run.done = true;
        run.cancelled = true;
        console.log(`[${run.label}] Order ${run.smmOrderId} cancelled`);
      } else if (smmStatus === 'partial') {
        run.done = true;
        run.smmStatus = 'partial';
        console.log(`[${run.label}] Order ${run.smmOrderId} partial`);
      }
    }
  }

  if (changed) saveRuns(allRuns);
}

/* =========================
   MAIN SCHEDULER
   FIX: Tightened to 3s for better run-time accuracy
========================= */
setInterval(async () => {
  const now = Date.now();

  for (let run of allRuns) {
    if (run.done || run.cancelled || run.paused) continue;

    const runTime = new Date(run.time).getTime();

    if (runTime <= now && !run.smmOrderId && !run.isExecuting) {
      await executeRun(run);
    }
  }

}, 3000); // FIX: was 10000ms — tighter scheduling

// Check SMM panel statuses every 30 seconds
setInterval(async () => {
  await checkRunStatuses();
}, 30000);

/* =========================
   CREATE ORDER
   FIX: Generate and return schedulerOrderId
        Accept + store name and startDelayHours
========================= */
app.post('/api/order', async (req, res) => {
  const { apiUrl, apiKey, link, services, name, startDelayHours } = req.body;

  if (!apiUrl || !apiKey || !link || !services) {
    return res.status(400).json({ error: 'Missing required fields: apiUrl, apiKey, link, services' });
  }

  // Validate services object
  if (typeof services !== 'object' || Array.isArray(services)) {
    return res.status(400).json({ error: 'services must be an object' });
  }

  // FIX: Generate a unique schedulerOrderId to track this order
  const schedulerOrderId = `SCHED-${Date.now()}-${randomUUID().slice(0, 8)}`;

  console.log(`[ORDER] Creating order schedulerOrderId=${schedulerOrderId} link=${link}`);

  addRuns(services, {
    apiUrl,
    apiKey,
    link,
    name: name || '',
    startDelayHours: Number(startDelayHours) || 0, // FIX: pass delay so runs are offset
    schedulerOrderId,
  });

  const runCount = Object.values(services).reduce((acc, svc) => {
    return acc + (svc?.runs?.length || 0);
  }, 0);

  return res.json({
    success: true,
    schedulerOrderId,   // FIX: return so frontend can track/control this order
    message: `Order scheduled — ${runCount} run(s) queued`,
    runCount,
  });
});

/* =========================
   ORDER CONTROL (pause / resume / cancel)
   FIX: This endpoint was completely missing — frontend api.ts calls it
========================= */
app.post('/api/order/control', async (req, res) => {
  const { schedulerOrderId, action } = req.body;

  if (!schedulerOrderId || !action) {
    return res.status(400).json({ error: 'Missing schedulerOrderId or action' });
  }

  if (!['pause', 'resume', 'cancel'].includes(action)) {
    return res.status(400).json({ error: 'action must be pause, resume, or cancel' });
  }

  const orderRuns = allRuns.filter(r => r.schedulerOrderId === schedulerOrderId);

  if (!orderRuns.length) {
    return res.status(404).json({ error: 'No runs found for this schedulerOrderId' });
  }

  let smmCancelCount = 0;
  let smmFailCount = 0;

  for (const run of orderRuns) {
    if (run.done) continue;

    if (action === 'pause') {
      run.paused = true;
      run.smmStatus = 'paused';
    } else if (action === 'resume') {
      run.paused = false;
      if (run.smmStatus === 'paused') run.smmStatus = 'pending';
    } else if (action === 'cancel') {
      run.cancelled = true;
      run.paused = false;
      run.smmStatus = 'cancelled';

      // Also cancel in SMM panel if already placed
      if (run.smmOrderId) {
        const result = await cancelOrderInSmmPanel({
          apiUrl: run.apiUrl,
          apiKey: run.apiKey,
          orderId: run.smmOrderId,
        });
        if (result.error) smmFailCount++;
        else smmCancelCount++;
      }
    }
  }

  saveRuns(allRuns);

  const completedRuns = orderRuns.filter(r => r.done && !r.cancelled).length;
  const runStatuses = orderRuns.map(r => {
    if (r.cancelled) return 'cancelled';
    if (r.done) return 'completed';
    return 'pending';
  });

  const statusMap = { pause: 'paused', resume: 'running', cancel: 'cancelled' };

  console.log(`[CONTROL] ${action} on schedulerOrderId=${schedulerOrderId} — ${orderRuns.length} runs affected`);

  return res.json({
    success: true,
    status: statusMap[action],
    completedRuns,
    runStatuses,
    smmPanelCancelled: smmCancelCount,
    smmPanelFailed: smmFailCount,
  });
});

/* =========================
   CANCEL ALL RUNS FOR A LINK (legacy — kept for compatibility)
========================= */
app.post('/api/cancel', async (req, res) => {
  const { link } = req.body;

  if (!link) {
    return res.status(400).json({ error: 'Missing link' });
  }

  let cancelledCount = 0;
  let smmCancelledCount = 0;
  let smmFailedCount = 0;
  const cancelResults = [];

  const runsToCancel = allRuns.filter(run => run.link === link && !run.done && !run.cancelled);

  for (const run of runsToCancel) {
    run.cancelled = true;
    run.smmStatus = 'cancelled';
    cancelledCount++;

    if (run.smmOrderId) {
      const result = await cancelOrderInSmmPanel({
        apiUrl: run.apiUrl,
        apiKey: run.apiKey,
        orderId: run.smmOrderId,
      });

      if (result.error) {
        smmFailedCount++;
        cancelResults.push({ orderId: run.smmOrderId, status: 'failed', error: result.error });
      } else {
        smmCancelledCount++;
        cancelResults.push({ orderId: run.smmOrderId, status: 'cancelled', response: result });
      }
    }
  }

  saveRuns(allRuns);

  console.log(`[CANCEL] Cancelled ${cancelledCount} runs for link: ${link}`);

  return res.json({
    success: true,
    cancelledRuns: cancelledCount,
    smmPanelCancelled: smmCancelledCount,
    smmPanelFailed: smmFailedCount,
    details: cancelResults,
  });
});

/* =========================
   CANCEL INDIVIDUAL RUN BY ID
========================= */
app.post('/api/cancel-run', async (req, res) => {
  const { runId } = req.body;

  if (!runId) {
    return res.status(400).json({ error: 'Missing runId' });
  }

  const run = allRuns.find(r => String(r.id) === String(runId));

  if (!run) {
    return res.status(404).json({ error: 'Run not found' });
  }

  if (run.done) {
    return res.json({ success: false, message: 'Run already completed' });
  }

  run.cancelled = true;
  run.smmStatus = 'cancelled';

  let smmResult = null;

  if (run.smmOrderId) {
    smmResult = await cancelOrderInSmmPanel({
      apiUrl: run.apiUrl,
      apiKey: run.apiKey,
      orderId: run.smmOrderId,
    });
  }

  saveRuns(allRuns);

  console.log(`[CANCEL-RUN] Cancelled run ${runId}`);

  return res.json({
    success: true,
    message: 'Run cancelled',
    smmPanelResult: smmResult,
  });
});

/* =========================
   GET RUN STATUSES FOR A LINK OR schedulerOrderId
   FIX: Also support schedulerOrderId lookup
========================= */
app.post('/api/run-statuses', (req, res) => {
  const { link, schedulerOrderId } = req.body;

  if (!link && !schedulerOrderId) {
    return res.status(400).json({ error: 'Missing link or schedulerOrderId' });
  }

  const orderRuns = schedulerOrderId
    ? allRuns.filter(run => run.schedulerOrderId === schedulerOrderId)
    : allRuns.filter(run => run.link === link);

  const completedRuns = orderRuns.filter(r => r.done && !r.cancelled).length;

  const overallStatus = (() => {
    if (orderRuns.every(r => r.done && !r.cancelled)) return 'completed';
    if (orderRuns.every(r => r.cancelled)) return 'cancelled';
    if (orderRuns.some(r => r.paused)) return 'paused';
    if (orderRuns.some(r => !r.done && !r.cancelled)) return 'running';
    return 'completed';
  })();

  const statuses = orderRuns.map(run => ({
    id: run.id,
    label: run.label,
    time: run.time,
    quantity: run.quantity,
    done: run.done,
    cancelled: run.cancelled,
    paused: run.paused || false,
    smmOrderId: run.smmOrderId,
    smmStatus: run.smmStatus || 'pending',
  }));

  return res.json({
    success: true,
    status: overallStatus,
    completedRuns,
    totalRuns: orderRuns.length,
    runs: statuses,
  });
});

/* =========================
   GET ALL RUNS (DEBUG)
========================= */
app.get('/api/all-runs', (req, res) => {
  return res.json({
    success: true,
    total: allRuns.length,
    runs: allRuns,
  });
});

/* =========================
   DELETE COMPLETED / CANCELLED RUNS (CLEANUP)
========================= */
app.post('/api/cleanup', (req, res) => {
  const before = allRuns.length;
  allRuns = allRuns.filter(r => !r.done && !r.cancelled);
  saveRuns(allRuns);
  return res.json({ success: true, removed: before - allRuns.length, remaining: allRuns.length });
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
      timeout: 20000,
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
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    totalRuns: allRuns.length,
    pendingRuns: allRuns.filter(r => !r.done && !r.cancelled).length,
    uptime: process.uptime(),
  });
});

/* =========================
   START SERVER
========================= */
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Loaded ${allRuns.length} runs from disk`);
});

/* =========================
   KEEP SERVER ALIVE (self-ping)
========================= */
setInterval(async () => {
  try {
    await axios.get(`http://localhost:${PORT}/health`);
    console.log('[PING] Self-ping ok');
  } catch (e) {}
}, 5 * 60 * 1000);
