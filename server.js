const express = require('express'); 
const cors = require('cors');
const axios = require('axios');
const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
mongoose.set('bufferCommands', false);

const app = express();
const JWT_SECRET = "mysecret123";
const PORT = process.env.PORT || 5000; 

app.use(cors());
app.use(express.json());

/* =========================
   🔥 MONGODB CONNECTION - FIXED
========================= */
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://harshprajapati6882_db_user:mbyjv1uPdKtLBz1l@devanush.tqknxqf.mongodb.net/smm-panel?retryWrites=true&w=majority';

mongoose.connect(MONGODB_URI, {
  serverSelectionTimeoutMS: 30000, // 🔥 increase timeout 
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
const UserSchema = new mongoose.Schema({
  email: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  role: { type: String, default: 'user' },
  createdAt: { type: Date, default: Date.now }
});

const User = mongoose.model('User', UserSchema);

const RunSchema = new mongoose.Schema({
  userId: { type: String, required: true, index: true },
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
});

const OrderSchema = new mongoose.Schema({
  userId: { type: String, required: true, index: true }, // 🔥 ADD THIS

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

const Run = mongoose.model('Run', RunSchema);
const Order = mongoose.model('Order', OrderSchema);

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
let commentsQueue = [];

let isExecutingViews = false;
let isExecutingLikes = false;
let isExecutingShares = false;
let isExecutingSaves = false;
let isExecutingComments = false;

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

  // 🔥 ADD THIS
  if (comments) {
    params.append('comments', comments);
  }

  const response = await axios.post(apiUrl, params.toString(), {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  });

  return response.data;
}
/* =========================
   ADD RUNS TO DATABASE
========================= */
async function addRuns(services, baseConfig, schedulerOrderId) {
  const runsForOrder = [];

  for (const [key, serviceConfig] of Object.entries(services)) {
    if (!serviceConfig) continue;

    const label = key.toUpperCase();
    const isViewService = label === 'VIEWS';

    for (const run of serviceConfig.runs) {
      let quantity;

// VIEWS
if (label === 'VIEWS') {
  if (!run.quantity || run.quantity < 100) continue;
  quantity = run.quantity;
}

// COMMENTS
else if (label === 'COMMENTS') {
  if (!run.comments) continue;

  let lines = run.comments
    .split('\n')
    .map(c => c.trim())
    .filter(c => c.length > 0);

  if (lines.length < 5) continue;

  // 🔥 LIMIT MAX TO 10
  if (lines.length > 10) {
  lines = lines.sort(() => Math.random() - 0.5).slice(0, 10);
}

  // 🔥 UPDATE COMMENTS AFTER TRIM
  run.comments = lines.join('\n');

  quantity = lines.length;
}

// OTHERS (likes, shares, saves)
else {
  if (!run.quantity || run.quantity <= 0) continue;
  quantity = run.quantity;
}
      const runData = new Run({
        userId: baseConfig.userId,
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
        createdAt: new Date(),
        executedAt: null,
        error: null,
        comments: run.comments || null,
      });

      await runData.save();
      runsForOrder.push(runData);
    }
  }

  return runsForOrder;
}

/* =========================
   EXECUTE RUN
========================= */
async function executeRun(run) {
   // 🔥 STOP IF ORDER CANCELLED
const order = await Order.findOne({ schedulerOrderId: run.schedulerOrderId });
   if (run.status === 'cancelled') {
  console.log(`[SKIP] Run already cancelled`);
  return;
}

if (!order || order.status === 'cancelled') {
  console.log(`[SKIP] Order cancelled → run skipped`);
  return;
}
  try {
     // 🔒 prevent same-type duplicate orders (IMPORTANT)
const activeSameType = await Run.findOne({
  link: run.link,
  label: run.label,
  status: { $in: ['processing'] },
schedulerOrderId: run.schedulerOrderId
});

if (activeSameType && activeSameType._id.toString() !== run._id.toString()) {
  console.log(`[${run.label}] Skipping - same type already active for this link`);

  // push back to queue
  if (run.label === 'VIEWS') viewsQueue.push(run);
  if (run.label === 'LIKES') likesQueue.push(run);
  if (run.label === 'SHARES') sharesQueue.push(run);
  if (run.label === 'SAVES') savesQueue.push(run);
  if (run.label === 'COMMENTS') commentsQueue.push(run);

  return;
}
    if (!run || !run._id) {
      console.warn(`[${run?.label}] Invalid run, skipping`);
      return;
    }

    if (!run.quantity || run.quantity <= 0) return;

    console.log(`[${run.label}] Executing run #${run.id}, quantity: ${run.quantity}`);

    // 🔥 SAFE UPDATE (no version conflict)
    await Run.updateOne(
      { _id: run._id },
      { $set: { status: 'processing' } }
    );

    await updateOrderStatus(run.schedulerOrderId);

    let payload = {
  apiUrl: run.apiUrl,
  apiKey: run.apiKey,
  service: run.service,
  link: run.link,
};

if (run.label === 'COMMENTS') {
  payload.comments = run.comments;
  payload.quantity = run.quantity;
} else {
  payload.quantity = run.quantity;
}

const result = await placeOrder(payload);

    if (result?.order) {
      console.log(`[${run.label}] SUCCESS - SMM Order ID: ${result.order}`);

      await Run.updateOne(
        { _id: run._id },
        {
          $set: {
            done: true,
            status: 'completed',
            smmOrderId: result.order,
            executedAt: new Date(),
          }
        }
      );

    } else {
      console.error(`[${run.label}] FAILED`, result);

      await Run.updateOne(
        { _id: run._id },
        {
          $set: {
            status: 'failed',
            error: result?.error || 'Unknown error'
          }
        }
      );
    }

  } catch (err) {
    console.error(`[${run.label}] ERROR`, err.response?.data || err.message);

    if (run?._id) {
      await Run.updateOne(
        { _id: run._id },
        {
          $set: {
            status: 'failed',
            error: err.response?.data?.error || err.message
          }
        }
      );
    }
  }

  await updateOrderStatus(run.schedulerOrderId);
}

/* =========================
   UPDATE ORDER STATUS
========================= */
async function updateOrderStatus(schedulerOrderId) {
  if (!schedulerOrderId) return;

  const orderRuns = await Run.find({ schedulerOrderId });
  const order = await Order.findOne({ schedulerOrderId });

  if (!order) return;

  const totalRuns = orderRuns.length;
  const completedRuns = orderRuns.filter(r => r.status === 'completed').length;
  const failedRuns = orderRuns.filter(r => r.status === 'failed').length;
  const processingRuns = orderRuns.filter(r => r.status === 'processing').length;
  const queuedRuns = orderRuns.filter(r => r.status === 'queued').length;

  if (completedRuns === totalRuns) {
    order.status = 'completed';
  } else if (failedRuns === totalRuns) {
    order.status = 'failed';
  } else if (processingRuns > 0 || completedRuns > 0 || queuedRuns > 0) {
    order.status = 'running';
  } else {
    order.status = 'pending';
  }

  order.completedRuns = completedRuns;
  order.totalRuns = totalRuns;
  order.lastUpdatedAt = new Date();
  order.runStatuses = orderRuns.map(r => r.status);

  await Order.updateOne(
  { schedulerOrderId },
  {
    $set: {
      status: order.status,
      completedRuns: order.completedRuns,
      totalRuns: order.totalRuns,
      lastUpdatedAt: new Date(),
      runStatuses: order.runStatuses
    }
  }
);
}

/* =========================
   🔥 QUEUE PROCESSORS
========================= */
async function processViewsQueue() {
  if (isExecutingViews || viewsQueue.length === 0) return;

  isExecutingViews = true;
  const run = viewsQueue.shift();

  console.log(`[VIEWS QUEUE] Processing run #${run.id}, Remaining: ${viewsQueue.length}`);

  try {
    const freshRun = await Run.findById(run._id);

    if (!freshRun || freshRun.status === 'cancelled') {
      console.log(`[VIEWS QUEUE] Skipped cancelled run`);
    } else {
      await executeRun(freshRun);
    }

  } catch (err) {
    console.error(`[VIEWS QUEUE] Error:`, err);
  }

  isExecutingViews = false;
  await new Promise(resolve => setTimeout(resolve, 8000));

  if (viewsQueue.length > 0) {
    setImmediate(() => processViewsQueue());
  }
}

async function processLikesQueue() {
  if (isExecutingLikes || likesQueue.length === 0) return;

  isExecutingLikes = true;
  const run = likesQueue.shift();

  console.log(`[LIKES QUEUE] Processing run #${run.id}, Remaining: ${likesQueue.length}`);

  try {
    const freshRun = await Run.findById(run._id);

    if (!freshRun || freshRun.status === 'cancelled') {
      console.log(`[LIKES QUEUE] Skipped cancelled run`);
    } else {
      await executeRun(freshRun);
    }

  } catch (err) {
    console.error(`[LIKES QUEUE] Error:`, err);
  }

  isExecutingLikes = false;
  await new Promise(resolve => setTimeout(resolve, 8000));

  if (likesQueue.length > 0) {
    setImmediate(() => processLikesQueue());
  }
}

async function processSharesQueue() {
  if (isExecutingShares || sharesQueue.length === 0) return;

  isExecutingShares = true;
  const run = sharesQueue.shift();

  console.log(`[SHARES QUEUE] Processing run #${run.id}, Remaining: ${sharesQueue.length}`);

  try {
    const freshRun = await Run.findById(run._id);

    if (!freshRun || freshRun.status === 'cancelled') {
      console.log(`[SHARES QUEUE] Skipped cancelled run`);
    } else {
      await executeRun(freshRun);
    }

  } catch (err) {
    console.error(`[SHARES QUEUE] Error:`, err);
  }

  isExecutingShares = false;
  await new Promise(resolve => setTimeout(resolve, 8000));

  if (sharesQueue.length > 0) {
    setImmediate(() => processSharesQueue());
  }
}

async function processSavesQueue() {
  if (isExecutingSaves || savesQueue.length === 0) return;

  isExecutingSaves = true;
  const run = savesQueue.shift();

  console.log(`[SAVES QUEUE] Processing run #${run.id}, Remaining: ${savesQueue.length}`);

  try {
    const freshRun = await Run.findById(run._id);

    if (!freshRun || freshRun.status === 'cancelled') {
      console.log(`[SAVES QUEUE] Skipped cancelled run`);
    } else {
      await executeRun(freshRun);
    }

  } catch (err) {
    console.error(`[SAVES QUEUE] Error:`, err);
  }

  isExecutingSaves = false;
  await new Promise(resolve => setTimeout(resolve, 8000));

  if (savesQueue.length > 0) {
    setImmediate(() => processSavesQueue());
  }
}

async function processCommentsQueue() {
  if (isExecutingComments || commentsQueue.length === 0) return;

  isExecutingComments = true;
  const run = commentsQueue.shift();

  console.log(`[COMMENTS QUEUE] Processing run #${run.id}, Remaining: ${commentsQueue.length}`);

  try {
    const freshRun = await Run.findById(run._id);

    if (!freshRun || freshRun.status === 'cancelled') {
      console.log(`[COMMENTS QUEUE] Skipped cancelled run`);
    } else {
      await executeRun(freshRun);
    }

  } catch (err) {
    console.error(`[COMMENTS QUEUE] Error:`, err);
  }

  isExecutingComments = false;
  await new Promise(resolve => setTimeout(resolve, 8000));

  if (commentsQueue.length > 0) {
    setImmediate(() => processCommentsQueue());
  }
}
/* =========================
   CHECK IF RUN IN QUEUE
========================= */
function isRunInQueue(runId) {
  return viewsQueue.some(r => r.id === runId) ||
         likesQueue.some(r => r.id === runId) ||
         sharesQueue.some(r => r.id === runId) ||
         savesQueue.some(r => r.id === runId) ||
         commentsQueue.some(r => r.id === runId);
}

/* =========================
   🔥 MAIN SCHEDULER
========================= */
mongoose.connection.once('open', () => {
  console.log("🚀 Scheduler started after DB connected");

  setInterval(async () => {
  try {
    const now = Date.now();
    let addedToQueue = { views: 0, likes: 0, shares: 0, saves: 0, comments: 0 };

    const allRuns = await Run.find({ 
      done: false,
      status: { $nin: ['completed', 'failed', 'cancelled', 'processing'] }
    });

    for (let run of allRuns) {
      if (run.status === 'queued' || isRunInQueue(run.id)) continue;
      const order = await Order.findOne({ schedulerOrderId: run.schedulerOrderId });

if (!order || order.status === 'cancelled') {
  continue; // 🔥 DO NOT ADD TO QUEUE
}

      const runTime = new Date(run.time).getTime();

      if (runTime <= now && run.status === 'pending') {
        
        if (run.label === 'VIEWS') {
          viewsQueue.push(run);
          run.status = 'queued';
          await run.save();
          addedToQueue.views++;
          console.log(`[SCHEDULER] Added VIEWS run #${run.id} to queue (qty: ${run.quantity})`);
        } 
        else if (run.label === 'LIKES') {
          likesQueue.push(run);
          run.status = 'queued';
          await run.save();
          addedToQueue.likes++;
          console.log(`[SCHEDULER] Added LIKES run #${run.id} to queue (qty: ${run.quantity})`);
        } 
        else if (run.label === 'SHARES') {
          sharesQueue.push(run);
          run.status = 'queued';
          await run.save();
          addedToQueue.shares++;
          console.log(`[SCHEDULER] Added SHARES run #${run.id} to queue (qty: ${run.quantity})`);
        } 
        else if (run.label === 'SAVES') {
          savesQueue.push(run);
          run.status = 'queued';
          await run.save();
          addedToQueue.saves++;
          console.log(`[SCHEDULER] Added SAVES run #${run.id} to queue (qty: ${run.quantity})`);
        }
        else if (run.label === 'COMMENTS') {
  commentsQueue.push(run);
  run.status = 'queued';
  await run.save();
  addedToQueue.comments++;
}
      }
    }

    if (addedToQueue.views + addedToQueue.likes + addedToQueue.shares + addedToQueue.saves > 0) {
      console.log(`[SCHEDULER] Added to queues - Views: ${addedToQueue.views}, Likes: ${addedToQueue.likes}, Shares: ${addedToQueue.shares}, Saves: ${addedToQueue.saves}, Comments: ${addedToQueue.comments}`);
    }

    if (viewsQueue.length > 0 && !isExecutingViews) processViewsQueue();
    if (likesQueue.length > 0 && !isExecutingLikes) processLikesQueue();
    if (sharesQueue.length > 0 && !isExecutingShares) processSharesQueue();
    if (savesQueue.length > 0 && !isExecutingSaves) processSavesQueue();
    if (commentsQueue.length > 0 && !isExecutingComments) processCommentsQueue();
  } catch (error) {
    console.error('[SCHEDULER] Error:', error);
  }
  }, 10000);
});

/* =========================
   API ENDPOINTS
========================= */
app.post('/api/auth/register', async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password required' });
    }

    const existing = await User.findOne({ email });
    if (existing) {
      return res.status(400).json({ error: 'User already exists' });
    }

    const hashedPassword = await bcrypt.hash(password, 10);

    const user = new User({
      email,
      password: hashedPassword,
      role: 'user'
    });

    await user.save();

    res.json({ success: true, message: 'User registered' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;

    const user = await User.findOne({ email });
    if (!user) {
      return res.status(400).json({ error: 'Invalid email' });
    }

    const isMatch = await bcrypt.compare(password, user.password);
    if (!isMatch) {
      return res.status(400).json({ error: 'Wrong password' });
    }

    const token = jwt.sign(
      { userId: user._id, role: user.role },
      JWT_SECRET,
      { expiresIn: '30d' }
    );

    res.json({
      success: true,
      token,
      user: {
        id: user._id,
        email: user.email,
        role: user.role
      }
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
function authMiddleware(req, res, next) {
  const token = req.headers.authorization;

  if (!token) {
    return res.status(401).json({ error: 'No token' });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    next();
  } catch (err) {
    return res.status(401).json({ error: 'Invalid token' });
  }
}
app.post('/api/order', authMiddleware, async (req, res) => {
  try {
    const { apiUrl, apiKey, link, services, name } = req.body;
const userId = req.user.userId; // 🔥 important
    console.log("SERVICES RECEIVED:", JSON.stringify(services, null, 2));

    if (!apiUrl || !apiKey || !link || !services) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    console.log('Creating new order...');

    const schedulerOrderId = `sched-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const runsForOrder = await addRuns(
  services,
  { apiUrl, apiKey, link, userId },
  schedulerOrderId
);

    const orderData = new Order({
      userId,
      schedulerOrderId,
      name: name || `Order ${schedulerOrderId}`,
      link,
      status: 'pending',
      totalRuns: runsForOrder.length,
      completedRuns: 0,
      runStatuses: runsForOrder.map(() => 'pending'),
      createdAt: new Date(),
      lastUpdatedAt: new Date(),
    });

    await orderData.save();

    console.log(`Order created: ${schedulerOrderId} with ${runsForOrder.length} runs`);

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

app.get('/api/order/status/:schedulerOrderId', authMiddleware, async (req, res) => {
  try {
    const { schedulerOrderId } = req.params;
    let order;

if (req.user.role === 'admin') {
  order = await Order.findOne({ schedulerOrderId });
} else {
  order = await Order.findOne({
    schedulerOrderId,
    userId: req.user.userId
  });
}
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

app.get('/api/orders/status', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.userId;

let allOrders;

if (req.user.role === 'admin') {
  allOrders = await Order.find().sort({ createdAt: -1 });
} else {
  allOrders = await Order.find({ userId }).sort({ createdAt: -1 });
}
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
    const orderRuns = await Run.find({ schedulerOrderId });

    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    if (action === 'cancel') {
      for (let run of orderRuns) {
        if (run.status === 'pending' || run.status === 'processing' || run.status === 'queued') {
          run.status = 'cancelled';
          run.done = true;
          await run.save();
          
          viewsQueue = viewsQueue.filter(r => r.id !== run.id);
          likesQueue = likesQueue.filter(r => r.id !== run.id);
          sharesQueue = sharesQueue.filter(r => r.id !== run.id);
          savesQueue = savesQueue.filter(r => r.id !== run.id);
          commentsQueue = commentsQueue.filter(r => r.id !== run.id);
        }
      }
      order.status = 'cancelled';
      await order.save();

      return res.json({
        success: true,
        status: 'cancelled',
        completedRuns: orderRuns.filter(r => r.status === 'completed').length,
        runStatuses: orderRuns.map(r => r.status),
      });
    }

    if (action === 'pause') {
      for (let run of orderRuns) {
        if (run.status === 'pending' || run.status === 'queued') {
          run.status = 'paused';
          await run.save();
          
          viewsQueue = viewsQueue.filter(r => r.id !== run.id);
          likesQueue = likesQueue.filter(r => r.id !== run.id);
          sharesQueue = sharesQueue.filter(r => r.id !== run.id);
          savesQueue = savesQueue.filter(r => r.id !== run.id);
        }
      }
      order.status = 'paused';
      await order.save();

      return res.json({
        success: true,
        status: 'paused',
        completedRuns: orderRuns.filter(r => r.status === 'completed').length,
        runStatuses: orderRuns.map(r => r.status),
      });
    }

    if (action === 'resume') {
      for (let run of orderRuns) {
        if (run.status === 'paused') {
          run.status = 'pending';
          await run.save();
        }
      }
      order.status = 'running';
      await order.save();

      return res.json({
        success: true,
        status: 'running',
        completedRuns: orderRuns.filter(r => r.status === 'completed').length,
        runStatuses: orderRuns.map(r => r.status),
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
},
comments: {
  queueLength: commentsQueue.length,
  isExecuting: isExecutingComments,
  pending: commentsQueue.map(r => ({
    id: r.id,
    quantity: r.quantity,
    time: r.time
  }))
}
  });
});

app.post('/api/runs/retry-stuck', async (req, res) => {
  try {
    const now = Date.now();
    let resetCount = 0;

    const allRuns = await Run.find({ done: false });

    for (let run of allRuns) {
      const runTime = new Date(run.time).getTime();
      if (runTime <= now && run.status === 'pending') {
        resetCount++;
      }
      if (run.status === 'queued' && !isRunInQueue(run.id)) {
        run.status = 'pending';
        await run.save();
        resetCount++;
      }
    }

    return res.json({
      success: true,
      resetCount,
      message: `Reset ${resetCount} stuck runs`
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.post('/api/scheduler/trigger', async (req, res) => {
  try {
    const now = Date.now();
    let addedToQueue = { views: 0, likes: 0, shares: 0, saves: 0, comments: 0 };

    const allRuns = await Run.find({ 
      done: false,
      status: { $nin: ['completed', 'failed', 'cancelled', 'processing', 'queued'] }
    });

    for (let run of allRuns) {
      if (isRunInQueue(run.id)) continue;
      const runTime = new Date(run.time).getTime();

      if (runTime <= now && run.status === 'pending') {
        if (run.label === 'VIEWS') {
          viewsQueue.push(run);
          run.status = 'queued';
          await run.save();
          addedToQueue.views++;
        } else if (run.label === 'LIKES') {
          likesQueue.push(run);
          run.status = 'queued';
          await run.save();
          addedToQueue.likes++;
        } else if (run.label === 'SHARES') {
          sharesQueue.push(run);
          run.status = 'queued';
          await run.save();
          addedToQueue.shares++;
        } else if (run.label === 'SAVES') {
          savesQueue.push(run);
          run.status = 'queued';
          await run.save();
          addedToQueue.saves++;
        } else if (run.label === 'COMMENTS') {
  commentsQueue.push(run);
  run.status = 'queued';
  await run.save();
  addedToQueue.comments++;
}
      }
    }

    if (viewsQueue.length > 0 && !isExecutingViews) processViewsQueue();
    if (likesQueue.length > 0 && !isExecutingLikes) processLikesQueue();
    if (sharesQueue.length > 0 && !isExecutingShares) processSharesQueue();
    if (savesQueue.length > 0 && !isExecutingSaves) processSavesQueue();
    if (commentsQueue.length > 0 && !isExecutingComments) processCommentsQueue();

    return res.json({
      success: true,
      addedToQueue,
      currentQueues: {
        views: viewsQueue.length,
        likes: likesQueue.length,
        shares: sharesQueue.length,
        saves: savesQueue.length
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
    await axios.get("https://backend-new-6tzb.onrender.com");
    console.log("[PING] Keeping server alive");
  } catch (e) {}
}, 5 * 60 * 1000);
app.get('/api/admin/users', authMiddleware, async (req, res) => {
  try {
    // only admin allowed
    if (req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Not allowed' });
    }

    const users = await User.find().select('-password');

    res.json({ users });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`========================================`);
  console.log(`Server running on port ${PORT}`);
  console.log(`Minimum views per run: ${MIN_VIEWS_PER_RUN}`);
  console.log(`4 Queue system initialized: VIEWS | LIKES | SHARES | SAVES`);
  console.log(`Scheduler runs every 10 seconds`);
  console.log(`========================================`);
});
