const express = require('express');
const cors = require('cors');
const axios = require('axios');
const fs = require('fs');

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
  fs.writeFileSync(DATA_FILE, JSON.stringify(runs, null, 2));
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
  });

  return response.data;
}

/* =========================
   🔥 CANCEL ORDER IN SMM PANEL
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
    });
    return response.data;
  } catch (err) {
    console.error('Status check failed:', err.message);
    return null;
  }
}

/* =========================
   ADD RUNS TO STORAGE
========================= */
function addRuns(services, baseConfig) {
  Object.entries(services).forEach(([key, serviceConfig]) => {
    if (!serviceConfig) return;

    const label = key.toUpperCase();

    serviceConfig.runs.forEach((run) => {
      allRuns.push({
        id: Date.now() + Math.random(),
        label,
        apiUrl: baseConfig.apiUrl,
        apiKey: baseConfig.apiKey,
        service: serviceConfig.serviceId,
        link: baseConfig.link,
        quantity: run.quantity,
        time: run.time,
        done: false,
        cancelled: false,
        retryCount: 0,
        isExecuting: false,
        smmOrderId: null,
        smmStatus: 'pending',
      });
    });
  });

  saveRuns(allRuns);
}

/* =========================
   EXECUTE RUN (SAFE + RETRY)
========================= */
async function executeRun(run) {
  if (run.cancelled || run.done || run.isExecuting) return;

  run.isExecuting = true;

  try {
    if (!run.quantity || run.quantity <= 0) {
      run.isExecuting = false;
      return;
    }

    console.log(`[${run.label}] Executing`, run);

    const result = await placeOrder(run);

    if (run.cancelled) {
      run.isExecuting = false;
      return;
    }

    if (result?.order) {
      console.log(`[${run.label}] SUCCESS`, result.order);
      run.smmOrderId = result.order;
      run.smmStatus = 'processing';
    } else {
      console.error(`[${run.label}] FAILED`, result);

      if (run.retryCount < 3 && !run.cancelled) {
        run.retryCount++;
        console.log(`[${run.label}] Retrying in 60 sec... Attempt ${run.retryCount}`);

        setTimeout(() => executeRun(run), 60000);
      } else {
        console.error(`[${run.label}] Max retries reached`);
        run.done = true;
        run.smmStatus = 'failed';
      }
    }

  } catch (err) {
    console.error(`[${run.label}] ERROR`, err.response?.data || err.message);

    if (run.retryCount < 3 && !run.cancelled) {
      run.retryCount++;
      console.log(`[${run.label}] Retrying after error... Attempt ${run.retryCount}`);

      setTimeout(() => executeRun(run), 60000);
    } else {
      console.error(`[${run.label}] Max retries reached after error`);
      run.done = true;
      run.smmStatus = 'failed';
    }
  }

  run.isExecuting = false;
}

/* =========================
   CHECK RUN STATUSES FROM SMM PANEL
========================= */
async function checkRunStatuses() {
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

      if (smmStatus === 'completed' || smmStatus === 'complete') {
        run.done = true;
        console.log(`[${run.label}] Order ${run.smmOrderId} completed in SMM panel`);
      } else if (smmStatus === 'canceled' || smmStatus === 'cancelled' || smmStatus === 'refunded') {
        run.done = true;
        run.cancelled = true;
        console.log(`[${run.label}] Order ${run.smmOrderId} cancelled in SMM panel`);
      } else if (smmStatus === 'partial') {
        run.done = true;
        run.smmStatus = 'partial';
        console.log(`[${run.label}] Order ${run.smmOrderId} partial in SMM panel`);
      }
    }
  }

  saveRuns(allRuns);
}

/* =========================
   MAIN SCHEDULER
========================= */
setInterval(async () => {
  const now = Date.now();

  for (let run of allRuns) {
    if (run.done || run.cancelled) continue;

    const runTime = new Date(run.time).getTime();

    if (runTime <= now && !run.smmOrderId && !run.isExecuting) {
      await executeRun(run);
    }
  }

  saveRuns(allRuns);

}, 10000);

// Check SMM panel statuses every 30 seconds
setInterval(async () => {
  await checkRunStatuses();
}, 30000);

/* =========================
   CREATE ORDER
========================= */
app.post('/api/order', async (req, res) => {
  const { apiUrl, apiKey, link, services } = req.body;

  if (!apiUrl || !apiKey || !link || !services) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  console.log('Saving runs for scheduler');

  addRuns(services, { apiUrl, apiKey, link });

  return res.json({
    success: true,
    message: 'Order scheduled (persistent)',
  });
});

/* =========================
   🔥 CANCEL ALL RUNS FOR A LINK (WITH SMM PANEL CANCELLATION)
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

  // Find all runs for this link
  const runsToCancel = allRuns.filter(run => run.link === link && !run.done);

  for (const run of runsToCancel) {
    // Mark as cancelled locally
    run.cancelled = true;
    run.smmStatus = 'cancelled';
    cancelledCount++;

    // 🔥 If order was already placed in SMM panel, cancel it there too
    if (run.smmOrderId) {
      console.log(`[CANCEL] Cancelling order ${run.smmOrderId} in SMM panel...`);
      
      const result = await cancelOrderInSmmPanel({
        apiUrl: run.apiUrl,
        apiKey: run.apiKey,
        orderId: run.smmOrderId,
      });

      if (result.error) {
        smmFailedCount++;
        cancelResults.push({
          orderId: run.smmOrderId,
          status: 'failed',
          error: result.error,
        });
      } else {
        smmCancelledCount++;
        cancelResults.push({
          orderId: run.smmOrderId,
          status: 'cancelled',
          response: result,
        });
      }
    }
  }

  saveRuns(allRuns);

  console.log(`Cancelled ${cancelledCount} runs for link: ${link}`);
  console.log(`SMM Panel: ${smmCancelledCount} cancelled, ${smmFailedCount} failed`);

  return res.json({
    success: true,
    cancelledRuns: cancelledCount,
    smmPanelCancelled: smmCancelledCount,
    smmPanelFailed: smmFailedCount,
    details: cancelResults,
  });
});

/* =========================
   🔥 CANCEL INDIVIDUAL RUN BY ID (WITH SMM PANEL CANCELLATION)
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
    return res.json({
      success: false,
      message: 'Run already completed',
    });
  }

  // Mark as cancelled locally
  run.cancelled = true;
  run.smmStatus = 'cancelled';

  let smmResult = null;

  // 🔥 If order was placed in SMM panel, cancel it there too
  if (run.smmOrderId) {
    console.log(`[CANCEL] Cancelling order ${run.smmOrderId} in SMM panel...`);
    
    smmResult = await cancelOrderInSmmPanel({
      apiUrl: run.apiUrl,
      apiKey: run.apiKey,
      orderId: run.smmOrderId,
    });
  }

  saveRuns(allRuns);

  console.log(`Cancelled run ${runId}`);

  return res.json({
    success: true,
    message: 'Run cancelled',
    smmPanelResult: smmResult,
  });
});

/* =========================
   GET RUN STATUSES FOR A LINK
========================= */
app.post('/api/run-statuses', (req, res) => {
  const { link } = req.body;

  if (!link) {
    return res.status(400).json({ error: 'Missing link' });
  }

  const orderRuns = allRuns.filter(run => run.link === link);

  const statuses = orderRuns.map(run => ({
    id: run.id,
    label: run.label,
    time: run.time,
    quantity: run.quantity,
    done: run.done,
    cancelled: run.cancelled,
    smmOrderId: run.smmOrderId,
    smmStatus: run.smmStatus || 'pending',
  }));

  return res.json({
    success: true,
    runs: statuses,
  });
});

/* =========================
   GET ALL RUNS (FOR DEBUGGING)
========================= */
app.get('/api/all-runs', (req, res) => {
  return res.json({
    success: true,
    total: allRuns.length,
    runs: allRuns,
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
    });

    return res.json(response.data);
  } catch (error) {
    return res.status(500).json({
      error: error.response?.data || error.message,
    });
  }
});

/* =========================
   START SERVER
========================= */
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});

/* =========================
   KEEP SERVER ALIVE
========================= */
setInterval(async () => {
  try {
    await axios.get("https://backend-new-6tzb.onrender.com");
    console.log("Self-ping to keep server alive");
  } catch (e) {}
}, 5 * 60 * 1000);
