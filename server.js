const express = require('express');
const cors = require('cors');
const axios = require('axios');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

const DATA_FILE = 'runs.json';
const ORDERS_FILE = 'orders.json';

/* =========================
   MINIMUM VIEWS PER RUN
========================= */
let MIN_VIEWS_PER_RUN = 100;

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

function loadOrders() {
  if (!fs.existsSync(ORDERS_FILE)) return [];
  try {
    return JSON.parse(fs.readFileSync(ORDERS_FILE));
  } catch (e) {
    return [];
  }
}

function saveOrders(orders) {
  fs.writeFileSync(ORDERS_FILE, JSON.stringify(orders, null, 2));
}

let allRuns = loadRuns();
let allOrders = loadOrders();

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
   ADD RUNS TO STORAGE
========================= */
function addRuns(services, baseConfig, schedulerOrderId) {
  const runsForOrder = [];

  Object.entries(services).forEach(([key, serviceConfig]) => {
    if (!serviceConfig) return;

    const label = key.toUpperCase();

    serviceConfig.runs.forEach((run, index) => {
      const quantity = Math.max(run.quantity, MIN_VIEWS_PER_RUN);

      const runData = {
        id: Date.now() + Math.random(),
        schedulerOrderId,
        label,
        apiUrl: baseConfig.apiUrl,
        apiKey: baseConfig.apiKey,
        service: serviceConfig.serviceId,
        link: baseConfig.link,
        quantity: quantity,
        time: run.time,
        done: false,
        status: 'pending',
        smmOrderId: null,
        createdAt: new Date().toISOString(),
        executedAt: null,
        error: null,
      };

      allRuns.push(runData);
      runsForOrder.push(runData);
    });
  });

  saveRuns(allRuns);
  return runsForOrder;
}

/* =========================
   EXECUTE RUN
========================= */
async function executeRun(run) {
  try {
    if (!run.quantity || run.quantity <= 0) return;

    console.log(`[${run.label}] Executing`, run);

    run.status = 'processing';
    saveRuns(allRuns);
    updateOrderStatus(run.schedulerOrderId);

    const result = await placeOrder(run);

    if (result?.order) {
      console.log(`[${run.label}] SUCCESS`, result.order);
      run.done = true;
      run.status = 'completed';
      run.smmOrderId = result.order;
      run.executedAt = new Date().toISOString();
    } else {
      console.error(`[${run.label}] FAILED`, result);
      run.status = 'failed';
      run.error = result?.error || 'Unknown error';
    }

  } catch (err) {
    console.error(`[${run.label}] ERROR`, err.response?.data || err.message);
    run.status = 'failed';
    run.error = err.response?.data?.error || err.message;
  }

  saveRuns(allRuns);
  updateOrderStatus(run.schedulerOrderId);
}

/* =========================
   UPDATE ORDER STATUS
========================= */
function updateOrderStatus(schedulerOrderId) {
  if (!schedulerOrderId) return;

  const orderRuns = allRuns.filter(r => r.schedulerOrderId === schedulerOrderId);
  const order = allOrders.find(o => o.schedulerOrderId === schedulerOrderId);

  if (!order) return;

  const totalRuns = orderRuns.length;
  const completedRuns = orderRuns.filter(r => r.status === 'completed').length;
  const failedRuns = orderRuns.filter(r => r.status === 'failed').length;
  const processingRuns = orderRuns.filter(r => r.status === 'processing').length;
  const pendingRuns = orderRuns.filter(r => r.status === 'pending').length;

  if (completedRuns === totalRuns) {
    order.status = 'completed';
  } else if (failedRuns === totalRuns) {
    order.status = 'failed';
  } else if (processingRuns > 0 || completedRuns > 0) {
    order.status = 'running';
  } else {
    order.status = 'pending';
  }

  order.completedRuns = completedRuns;
  order.totalRuns = totalRuns;
  order.lastUpdatedAt = new Date().toISOString();

  order.runStatuses = orderRuns.map(r => r.status);

  saveOrders(allOrders);
}

/* =========================
   MAIN SCHEDULER (EVERY 10 SEC)
========================= */
setInterval(async () => {
  const now = Date.now();

  for (let run of allRuns) {
    if (run.done || run.status === 'completed' || run.status === 'failed') continue;

    const runTime = new Date(run.time).getTime();

    if (runTime <= now && run.status === 'pending') {
      await executeRun(run);
    }
  }

  saveRuns(allRuns);

}, 10000);

/* =========================
   CREATE ORDER
========================= */
app.post('/api/order', async (req, res) => {
  const { apiUrl, apiKey, link, services, name } = req.body;

  if (!apiUrl || !apiKey || !link || !services) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  console.log('Saving runs for scheduler');

  const schedulerOrderId = `sched-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

  const runsForOrder = addRuns(services, { apiUrl, apiKey, link }, schedulerOrderId);

  const orderData = {
    schedulerOrderId,
    name: name || `Order ${schedulerOrderId}`,
    link,
    status: 'pending',
    totalRuns: runsForOrder.length,
    completedRuns: 0,
    runStatuses: runsForOrder.map(() => 'pending'),
    createdAt: new Date().toISOString(),
    lastUpdatedAt: new Date().toISOString(),
  };

  allOrders.push(orderData);
  saveOrders(allOrders);

  return res.json({
    success: true,
    message: 'Order scheduled (persistent)',
    schedulerOrderId,
    status: 'pending',
    completedRuns: 0,
    totalRuns: runsForOrder.length,
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
   GET ORDER STATUS
========================= */
app.get('/api/order/status/:schedulerOrderId', (req, res) => {
  const { schedulerOrderId } = req.params;

  const order = allOrders.find(o => o.schedulerOrderId === schedulerOrderId);
  const orderRuns = allRuns.filter(r => r.schedulerOrderId === schedulerOrderId);

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
});

/* =========================
   GET ALL ORDERS STATUS
========================= */
app.get('/api/orders/status', (req, res) => {
  const ordersWithRuns = allOrders.map(order => {
    const orderRuns = allRuns.filter(r => r.schedulerOrderId === order.schedulerOrderId);
    return {
      ...order,
      runs: orderRuns.map(r => ({
        id: r.id,
        label: r.label,
        quantity: r.quantity,
        time: r.time,
        status: r.status,
        smmOrderId: r.smmOrderId,
      })),
    };
  });

  return res.json({
    total: allOrders.length,
    orders: ordersWithRuns,
  });
});

/* =========================
   ORDER CONTROL (PAUSE/RESUME/CANCEL)
========================= */
app.post('/api/order/control', (req, res) => {
  const { schedulerOrderId, action } = req.body;

  if (!schedulerOrderId || !action) {
    return res.status(400).json({ error: 'Missing schedulerOrderId or action' });
  }

  const order = allOrders.find(o => o.schedulerOrderId === schedulerOrderId);
  const orderRuns = allRuns.filter(r => r.schedulerOrderId === schedulerOrderId);

  if (!order) {
    return res.status(404).json({ error: 'Order not found' });
  }

  if (action === 'cancel') {
    orderRuns.forEach(run => {
      if (run.status === 'pending' || run.status === 'processing') {
        run.status = 'cancelled';
        run.done = true;
      }
    });
    order.status = 'cancelled';
    saveRuns(allRuns);
    saveOrders(allOrders);

    return res.json({
      success: true,
      status: 'cancelled',
      completedRuns: orderRuns.filter(r => r.status === 'completed').length,
      runStatuses: orderRuns.map(r => r.status),
    });
  }

  if (action === 'pause') {
    orderRuns.forEach(run => {
      if (run.status === 'pending') {
        run.status = 'paused';
      }
    });
    order.status = 'paused';
    saveRuns(allRuns);
    saveOrders(allOrders);

    return res.json({
      success: true,
      status: 'paused',
      completedRuns: orderRuns.filter(r => r.status === 'completed').length,
      runStatuses: orderRuns.map(r => r.status),
    });
  }

  if (action === 'resume') {
    orderRuns.forEach(run => {
      if (run.status === 'paused') {
        run.status = 'pending';
      }
    });
    order.status = 'running';
    saveRuns(allRuns);
    saveOrders(allOrders);

    return res.json({
      success: true,
      status: 'running',
      completedRuns: orderRuns.filter(r => r.status === 'completed').length,
      runStatuses: orderRuns.map(r => r.status),
    });
  }

  return res.status(400).json({ error: 'Invalid action' });
});

/* =========================
   GET ORDER RUNS
========================= */
app.get('/api/order/runs/:schedulerOrderId', (req, res) => {
  const { schedulerOrderId } = req.params;

  const orderRuns = allRuns.filter(r => r.schedulerOrderId === schedulerOrderId);

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
    })),
  });
});

/* =========================
   GET/SET MINIMUM VIEWS
========================= */
app.get('/api/settings/min-views', (req, res) => {
  return res.json({
    minViewsPerRun: MIN_VIEWS_PER_RUN,
  });
});

app.post('/api/settings/min-views', (req, res) => {
  const { minViewsPerRun } = req.body;

  if (typeof minViewsPerRun !== 'number' || minViewsPerRun < 1) {
    return res.status(400).json({ error: 'Invalid minViewsPerRun value' });
  }

  MIN_VIEWS_PER_RUN = Math.floor(minViewsPerRun);
  console.log(`Minimum views per run updated to: ${MIN_VIEWS_PER_RUN}`);

  return res.json({
    success: true,
    minViewsPerRun: MIN_VIEWS_PER_RUN,
  });
});

/* =========================
   START SERVER
========================= */
setInterval(async () => {
  try {
    await axios.get("https://backend-new-6tzb.onrender.com");
    console.log("Self-ping to keep server alive");
  } catch (e) {}
}, 5 * 60 * 1000);

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Minimum views per run: ${MIN_VIEWS_PER_RUN}`);
});
