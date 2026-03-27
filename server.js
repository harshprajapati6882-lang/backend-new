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
   🔥 4 SEPARATE QUEUES + FLAGS
========================= */
let viewsQueue = [];
let likesQueue = [];
let sharesQueue = [];
let savesQueue = [];

let isExecutingViews = false;
let isExecutingLikes = false;
let isExecutingShares = false;
let isExecutingSaves = false;

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
   ADD RUNS TO STORAGE - 🔥 FIXED
========================= */
function addRuns(services, baseConfig, schedulerOrderId) {
  const runsForOrder = [];

  Object.entries(services).forEach(([key, serviceConfig]) => {
    if (!serviceConfig) return;

    const label = key.toUpperCase();
    
    // 🔥 FIX: Only apply MIN_VIEWS_PER_RUN to VIEWS, not to engagement
    const isViewService = label === 'VIEWS';

    serviceConfig.runs.forEach((run, index) => {
      // 🔥 Apply minimum only for views, allow 0 for others
      const quantity = isViewService 
        ? Math.max(run.quantity, MIN_VIEWS_PER_RUN)
        : run.quantity;

      // 🔥 Skip creating run if quantity is 0 (for likes/shares/saves)
      if (quantity === 0) return;

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
   🔥 QUEUE PROCESSORS (4 SEPARATE WORKERS)
========================= */
async function processViewsQueue() {
  if (isExecutingViews || viewsQueue.length === 0) return;
  
  isExecutingViews = true;
  const run = viewsQueue.shift();
  
  console.log(`[VIEWS QUEUE] Processing run #${run.id}, Queue length: ${viewsQueue.length}`);
  
  await executeRun(run);
  
  isExecutingViews = false;
  
  // Process next run if any
  if (viewsQueue.length > 0) {
    setImmediate(() => processViewsQueue());
  }
}

async function processLikesQueue() {
  if (isExecutingLikes || likesQueue.length === 0) return;
  
  isExecutingLikes = true;
  const run = likesQueue.shift();
  
  console.log(`[LIKES QUEUE] Processing run #${run.id}, Queue length: ${likesQueue.length}`);
  
  await executeRun(run);
  
  isExecutingLikes = false;
  
  // Process next run if any
  if (likesQueue.length > 0) {
    setImmediate(() => processLikesQueue());
  }
}

async function processSharesQueue() {
  if (isExecutingShares || sharesQueue.length === 0) return;
  
  isExecutingShares = true;
  const run = sharesQueue.shift();
  
  console.log(`[SHARES QUEUE] Processing run #${run.id}, Queue length: ${sharesQueue.length}`);
  
  await executeRun(run);
  
  isExecutingShares = false;
  
  // Process next run if any
  if (sharesQueue.length > 0) {
    setImmediate(() => processSharesQueue());
  }
}

async function processSavesQueue() {
  if (isExecutingSaves || savesQueue.length === 0) return;
  
  isExecutingSaves = true;
  const run = savesQueue.shift();
  
  console.log(`[SAVES QUEUE] Processing run #${run.id}, Queue length: ${savesQueue.length}`);
  
  await executeRun(run);
  
  isExecutingSaves = false;
  
  // Process next run if any
  if (savesQueue.length > 0) {
    setImmediate(() => processSavesQueue());
  }
}

/* =========================
   🔥 MAIN SCHEDULER - ADDS TO QUEUES
========================= */
setInterval(async () => {
  const now = Date.now();

  for (let run of allRuns) {
    // Skip completed/failed/cancelled/queued runs
    if (run.done || run.status === 'completed' || run.status === 'failed' || run.status === 'queued') continue;

    const runTime = new Date(run.time).getTime();

    // Check if run time has arrived
    if (runTime <= now && run.status === 'pending') {
      
      // 🔥 Add to appropriate queue based on label
      if (run.label === 'VIEWS') {
        viewsQueue.push(run);
        run.status = 'queued';
        console.log(`[SCHEDULER] Added VIEWS run #${run.id} to queue`);
        processViewsQueue(); // Trigger processor
      } 
      else if (run.label === 'LIKES') {
        likesQueue.push(run);
        run.status = 'queued';
        console.log(`[SCHEDULER] Added LIKES run #${run.id} to queue`);
        processLikesQueue(); // Trigger processor
      } 
      else if (run.label === 'SHARES') {
        sharesQueue.push(run);
        run.status = 'queued';
        console.log(`[SCHEDULER] Added SHARES run #${run.id} to queue`);
        processSharesQueue(); // Trigger processor
      } 
      else if (run.label === 'SAVES') {
        savesQueue.push(run);
        run.status = 'queued';
        console.log(`[SCHEDULER] Added SAVES run #${run.id} to queue`);
        processSavesQueue(); // Trigger processor
      }
    }
  }

  saveRuns(allRuns);

}, 10000); // Check every 10 seconds

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
      if (run.status === 'pending' || run.status === 'processing' || run.status === 'queued') {
        run.status = 'cancelled';
        run.done = true;
        
        // 🔥 Remove from queues if present
        viewsQueue = viewsQueue.filter(r => r.id !== run.id);
        likesQueue = likesQueue.filter(r => r.id !== run.id);
        sharesQueue = sharesQueue.filter(r => r.id !== run.id);
        savesQueue = savesQueue.filter(r => r.id !== run.id);
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
      if (run.status === 'pending' || run.status === 'queued') {
        run.status = 'paused';
        
        // 🔥 Remove from queues if present
        viewsQueue = viewsQueue.filter(r => r.id !== run.id);
        likesQueue = likesQueue.filter(r => r.id !== run.id);
        sharesQueue = sharesQueue.filter(r => r.id !== run.id);
        savesQueue = savesQueue.filter(r => r.id !== run.id);
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
        run.status = 'pending'; // Will be picked up by scheduler on next check
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
   🔥 GET QUEUE STATUS (NEW ENDPOINT)
========================= */
app.get('/api/queues/status', (req, res) => {
  return res.json({
    views: {
      queueLength: viewsQueue.length,
      isExecuting: isExecutingViews,
      pending: viewsQueue.map(r => ({ id: r.id, quantity: r.quantity, time: r.time }))
    },
    likes: {
      queueLength: likesQueue.length,
      isExecuting: isExecutingLikes,
      pending: likesQueue.map(r => ({ id: r.id, quantity: r.quantity, time: r.time }))
    },
    shares: {
      queueLength: sharesQueue.length,
      isExecuting: isExecutingShares,
      pending: sharesQueue.map(r => ({ id: r.id, quantity: r.quantity, time: r.time }))
    },
    saves: {
      queueLength: savesQueue.length,
      isExecuting: isExecutingSaves,
      pending: savesQueue.map(r => ({ id: r.id, quantity: r.quantity, time: r.time }))
    }
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
  console.log(`4 Queue system initialized: VIEWS | LIKES | SHARES | SAVES`);
});
