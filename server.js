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
  return JSON.parse(fs.readFileSync(DATA_FILE));
}

function saveRuns(runs) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(runs, null, 2));
}

let allRuns = loadRuns();

/* =========================
   PLACE ORDER + CHECK STATUS
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
        smmOrderId: null, // 🔥 NEW: Track SMM panel order ID
        smmStatus: 'pending', // 🔥 NEW: Track actual SMM status
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
      run.smmOrderId = result.order; // 🔥 Store SMM order ID
      run.smmStatus = 'processing'; // 🔥 Set initial status
      // Don't mark as done yet - wait for actual completion
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
   🔥 NEW: CHECK RUN STATUSES
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

      // Mark as done only when SMM panel reports completion
      if (smmStatus === 'completed' || smmStatus === 'complete') {
        run.done = true;
        console.log(`[${run.label}] Order ${run.smmOrderId} completed in SMM panel`);
      } else if (smmStatus === 'canceled' || smmStatus === 'cancelled') {
        run.done = true;
        run.cancelled = true;
        console.log(`[${run.label}] Order ${run.smmOrderId} cancelled in SMM panel`);
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

  // Execute pending runs
  for (let run of allRuns) {
    if (run.done || run.cancelled) continue;

    const runTime = new Date(run.time).getTime();

    if (runTime <= now && !run.smmOrderId) {
      await executeRun(run);
    }
  }

  saveRuns(allRuns);
}, 10000);

// 🔥 NEW: Check statuses every 30 seconds
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
   🔥 CANCEL INDIVIDUAL RUN
========================= */
app.post('/api/cancel-run', (req, res) => {
  const { runId } = req.body;

  if (!runId) {
    return res.status(400).json({ error: 'Missing runId' });
  }

  const run = allRuns.find(r => r.id === runId);

  if (!run) {
    return res.status(404).json({ error: 'Run not found' });
  }

  if (!run.done) {
    run.cancelled = true;
    saveRuns(allRuns);
    console.log(`Cancelled run ${runId}`);
  }

  return res.json({
    success: true,
    message: 'Run cancelled',
  });
});

/* =========================
   🔥 CANCEL ORDER (ALL RUNS FOR LINK)
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
      cancelledCount++;
    }
  });

  saveRuns(allRuns);

  console.log(`Cancelled ${cancelledCount} runs for link: ${link}`);

  return res.json({
    success: true,
    cancelledRuns: cancelledCount,
  });
});

/* =========================
   🔥 GET RUN STATUSES FOR ORDER
========================= */
app.post('/api/run-statuses', (req, res) => {
  const { link } = req.body;

  if (!link) {
    return res.status(400).json({ error: 'Missing link' });
  }

  const orderRuns = allRuns.filter(run => run.link === link);

  const statuses = orderRuns.map(run => ({
    id: run.id,
    time: run.time,
    quantity: run.quantity,
    done: run.done,
    cancelled: run.cancelled,
    smmOrderId: run.smmOrderId,
    smmStatus: run.smmStatus,
  }));

  return res.json({
    success: true,
    runs: statuses,
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
