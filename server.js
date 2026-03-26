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
function addRuns(services, baseConfig) {
  Object.entries(services).forEach(([key, serviceConfig]) => {
    if (!serviceConfig) return;

    const label = key.toUpperCase();

    serviceConfig.runs.forEach((run, index) => {
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
      });
    });
  });

  saveRuns(allRuns);
}

/* =========================
   EXECUTE RUN
========================= */
async function executeRun(run) {
  try {
    if (!run.quantity || run.quantity <= 0) return;

    console.log(`[${run.label}] Executing`, run);

    const result = await placeOrder(run);

    if (result?.order) {
      console.log(`[${run.label}] SUCCESS`, result.order);
      run.done = true;
    } else {
      console.error(`[${run.label}] FAILED`, result);
    }

  } catch (err) {
    console.error(`[${run.label}] ERROR`, err.response?.data || err.message);
  }
}

/* =========================
   MAIN SCHEDULER (EVERY 10 SEC)
========================= */
setInterval(async () => {
  const now = Date.now();

  for (let run of allRuns) {
    if (run.done) continue;

    const runTime = new Date(run.time).getTime();

    if (runTime <= now) {
      await executeRun(run);
    }
  }

  saveRuns(allRuns);

}, 10000); // every 10 sec

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
setInterval(async () => {
  try {
    await axios.get("https://backend-y30y.onrender.com");
    console.log("Self-ping to keep server alive");
  } catch (e) {}
}, 5 * 60 * 1000); // every 5 minutes
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});
