// 🔥 Increase memory limit for Node.js
if (!process.env.NODE_OPTIONS) {
  process.env.NODE_OPTIONS = '--max-old-space-size=450';
}
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cookieParser = require('cookie-parser');

mongoose.set('bufferCommands', false);

const app = express();
const PORT = process.env.PORT || 5000;

// ============================================
// 🔐 JWT SECRET - Change this in production!
// ============================================
const JWT_SECRET = process.env.JWT_SECRET || 'gotham-smm-secret-key-change-in-production-2024';
const JWT_EXPIRES_IN = '7d';

// ============================================
// 🌐 CORS SETUP - Allow your frontend
// ============================================
app.use(cors({
  origin: [
    'https://devanush-final.vercel.app',
    'http://localhost:5173',
    'http://localhost:3000'
  ],
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.use(cookieParser());

/* =========================
   🔥 MONGODB CONNECTION
========================= */
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://harshprajapati6882_db_user:mbyjv1uPdKtLBz1l@devanush.tqknxqf.mongodb.net/smm-panel?retryWrites=true&w=majority';

mongoose.connect(MONGODB_URI, {
  serverSelectionTimeoutMS: 30000,
})
.then(() => {
  console.log('✅ MongoDB Connected Successfully');
  seedAdmin(); // 🔥 Create admin on first run
})
.catch(err => {
  console.error('❌ MongoDB Connection Error:', err);
});

/* =========================
   🔥 MONGODB SCHEMAS
========================= */

// ============================================
// 👤 USER SCHEMA - NEW
// ============================================
const UserSchema = new mongoose.Schema({
  username: { type: String, required: true, unique: true, trim: true, minlength: 3, maxlength: 30 },
  email: { type: String, required: true, unique: true, trim: true, lowercase: true },
  password: { type: String, required: true, minlength: 6 },
  role: { type: String, enum: ['admin', 'user'], default: 'user' },
  status: { type: String, enum: ['active', 'banned'], default: 'active' },
  createdAt: { type: Date, default: Date.now },
  lastLoginAt: { type: Date, default: null },
});

const User = mongoose.model('User', UserSchema);

// ============================================
// 📋 RUN SCHEMA - UPDATED with userId
// ============================================
const RunSchema = new mongoose.Schema({
  id: { type: Number, required: true, index: true },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
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

// ============================================
// 📦 ORDER SCHEMA - UPDATED with userId
// ============================================
const OrderSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
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

// ============================================
// 🔗 API PANEL SCHEMA - NEW
// ============================================
const ApiPanelSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  name: { type: String, required: true, trim: true },
  url: { type: String, required: true, trim: true },
  key: { type: String, required: true },
  status: { type: String, enum: ['Active', 'Inactive'], default: 'Active' },
  services: { type: Array, default: [] },
  lastFetchAt: { type: String, default: null },
  lastFetchError: { type: String, default: null },
  createdAt: { type: Date, default: Date.now },
});

// ============================================
// 📁 BUNDLE SCHEMA - NEW
// ============================================
const BundleSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  name: { type: String, required: true, trim: true },
  apiId: { type: String, required: true },
  serviceIds: {
    views: { type: String, default: '' },
    likes: { type: String, default: '' },
    shares: { type: String, default: '' },
    saves: { type: String, default: '' },
    comments: { type: String, default: '' },
  },
  createdAt: { type: Date, default: Date.now },
});

const ApiPanel = mongoose.model('ApiPanel', ApiPanelSchema);
const Bundle = mongoose.model('Bundle', BundleSchema);
const Run = mongoose.model('Run', RunSchema);
const Order = mongoose.model('Order', OrderSchema);

/* =========================
   🔐 SEED ADMIN USER
========================= */
async function seedAdmin() {
  try {
    const existingAdmin = await User.findOne({ role: 'admin' });
    if (existingAdmin) {
      console.log('✅ Admin already exists:', existingAdmin.username);
      return;
    }

    const hashedPassword = await bcrypt.hash('admin123456', 12);
    const admin = new User({
      username: 'admin',
      email: 'admin@gotham.com',
      password: hashedPassword,
      role: 'admin',
      status: 'active',
    });

    await admin.save();
    console.log('✅ Admin user created!');
    console.log('   Username: admin');
    console.log('   Email: admin@gotham.com');
    console.log('   Password: admin123456');
    console.log('   ⚠️ CHANGE THIS PASSWORD AFTER FIRST LOGIN!');
  } catch (error) {
    console.error('❌ Error seeding admin:', error.message);
  }
}

/* =========================
   🔐 AUTH MIDDLEWARE
========================= */
function authenticateToken(req, res, next) {
  // Check Authorization header first
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.startsWith('Bearer ') 
    ? authHeader.split(' ')[1] 
    : null;

  if (!token) {
    return res.status(401).json({ error: 'Access denied. No token provided.' });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    next();
  } catch (error) {
    return res.status(403).json({ error: 'Invalid or expired token.' });
  }
}

// Middleware: Check if user is admin
function requireAdmin(req, res, next) {
  if (req.user.role !== 'admin') {
    return res.status(403).json({ error: 'Admin access required.' });
  }
  next();
}

// Middleware: Check if user is not banned
async function checkNotBanned(req, res, next) {
  try {
    const user = await User.findById(req.user.userId);
    if (!user) {
      return res.status(401).json({ error: 'User not found.' });
    }
    if (user.status === 'banned') {
      return res.status(403).json({ error: 'Your account has been banned. Contact admin.' });
    }
    next();
  } catch (error) {
    return res.status(500).json({ error: 'Server error checking user status.' });
  }
}

// Combined middleware for protected routes
const protect = [authenticateToken, checkNotBanned];
const adminOnly = [authenticateToken, checkNotBanned, requireAdmin];

/* =========================
   🔐 AUTH ROUTES
========================= */

// ============================================
// POST /api/auth/signup
// ============================================
app.post('/api/auth/signup', async (req, res) => {
  try {
    const { username, email, password } = req.body;

    // Validation
    if (!username || !email || !password) {
      return res.status(400).json({ error: 'Username, email, and password are required.' });
    }

    if (username.length < 3 || username.length > 30) {
      return res.status(400).json({ error: 'Username must be 3-30 characters.' });
    }

    if (password.length < 6) {
      return res.status(400).json({ error: 'Password must be at least 6 characters.' });
    }

    // Email validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ error: 'Invalid email format.' });
    }

    // Username validation (alphanumeric + underscore only)
    const usernameRegex = /^[a-zA-Z0-9_]+$/;
    if (!usernameRegex.test(username)) {
      return res.status(400).json({ error: 'Username can only contain letters, numbers, and underscores.' });
    }

    // Check if user exists
    const existingUser = await User.findOne({ 
      $or: [{ email: email.toLowerCase() }, { username }] 
    });

    if (existingUser) {
      if (existingUser.email === email.toLowerCase()) {
        return res.status(400).json({ error: 'Email already registered.' });
      }
      return res.status(400).json({ error: 'Username already taken.' });
    }

    // Hash password
    const hashedPassword = await bcrypt.hash(password, 12);

    // Create user
    const user = new User({
      username,
      email: email.toLowerCase(),
      password: hashedPassword,
      role: 'user',
      status: 'active',
    });

    await user.save();

    // Generate token
    const token = jwt.sign(
      { userId: user._id, username: user.username, role: user.role },
      JWT_SECRET,
      { expiresIn: JWT_EXPIRES_IN }
    );

    console.log(`✅ New user registered: ${username}`);

    return res.status(201).json({
      success: true,
      message: 'Account created successfully!',
      token,
      user: {
        id: user._id,
        username: user.username,
        email: user.email,
        role: user.role,
        status: user.status,
        createdAt: user.createdAt,
      },
    });
  } catch (error) {
    console.error('[SIGNUP] Error:', error);
    return res.status(500).json({ error: 'Server error during signup.' });
  }
});

// ============================================
// POST /api/auth/login
// ============================================
app.post('/api/auth/login', async (req, res) => {
  try {
    const { login, password } = req.body;

    if (!login || !password) {
      return res.status(400).json({ error: 'Username/email and password are required.' });
    }

    // Find user by email or username
    const user = await User.findOne({
      $or: [
        { email: login.toLowerCase() },
        { username: login }
      ]
    });

    if (!user) {
      return res.status(401).json({ error: 'Invalid credentials.' });
    }

    // Check if banned
    if (user.status === 'banned') {
      return res.status(403).json({ error: 'Your account has been banned. Contact admin.' });
    }

    // Check password
    const isMatch = await bcrypt.compare(password, user.password);
    if (!isMatch) {
      return res.status(401).json({ error: 'Invalid credentials.' });
    }

    // Update last login
    user.lastLoginAt = new Date();
    await user.save();

    // Generate token
    const token = jwt.sign(
      { userId: user._id, username: user.username, role: user.role },
      JWT_SECRET,
      { expiresIn: JWT_EXPIRES_IN }
    );

    console.log(`✅ User logged in: ${user.username}`);

    return res.json({
      success: true,
      message: 'Login successful!',
      token,
      user: {
        id: user._id,
        username: user.username,
        email: user.email,
        role: user.role,
        status: user.status,
        createdAt: user.createdAt,
      },
    });
  } catch (error) {
    console.error('[LOGIN] Error:', error);
    return res.status(500).json({ error: 'Server error during login.' });
  }
});

// ============================================
// GET /api/auth/me - Get current user info
// ============================================
app.get('/api/auth/me', ...protect, async (req, res) => {
  try {
    const user = await User.findById(req.user.userId).select('-password');
    if (!user) {
      return res.status(404).json({ error: 'User not found.' });
    }

    return res.json({
      success: true,
      user: {
        id: user._id,
        username: user.username,
        email: user.email,
        role: user.role,
        status: user.status,
        createdAt: user.createdAt,
        lastLoginAt: user.lastLoginAt,
      },
    });
  } catch (error) {
    return res.status(500).json({ error: 'Server error.' });
  }
});

// ============================================
// POST /api/auth/change-password
// ============================================
app.post('/api/auth/change-password', ...protect, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body;

    if (!currentPassword || !newPassword) {
      return res.status(400).json({ error: 'Current and new password are required.' });
    }

    if (newPassword.length < 6) {
      return res.status(400).json({ error: 'New password must be at least 6 characters.' });
    }

    const user = await User.findById(req.user.userId);
    if (!user) {
      return res.status(404).json({ error: 'User not found.' });
    }

    const isMatch = await bcrypt.compare(currentPassword, user.password);
    if (!isMatch) {
      return res.status(401).json({ error: 'Current password is incorrect.' });
    }

    user.password = await bcrypt.hash(newPassword, 12);
    await user.save();

    return res.json({ success: true, message: 'Password changed successfully!' });
  } catch (error) {
    return res.status(500).json({ error: 'Server error.' });
  }
});

/* =========================
   👑 ADMIN ROUTES
========================= */

// ============================================
// GET /api/admin/users - List all users
// ============================================
app.get('/api/admin/users', ...adminOnly, async (req, res) => {
  try {
    const users = await User.find().select('-password').sort({ createdAt: -1 });

    // Get order counts for each user
    const usersWithStats = await Promise.all(users.map(async (user) => {
      const orderCount = await Order.countDocuments({ userId: user._id });
      const runCount = await Run.countDocuments({ userId: user._id });

      return {
        id: user._id,
        username: user.username,
        email: user.email,
        role: user.role,
        status: user.status,
        createdAt: user.createdAt,
        lastLoginAt: user.lastLoginAt,
        orderCount,
        runCount,
      };
    }));

    return res.json({ success: true, users: usersWithStats, total: users.length });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// ============================================
// POST /api/admin/users/:userId/ban
// ============================================
app.post('/api/admin/users/:userId/ban', ...adminOnly, async (req, res) => {
  try {
    const { userId } = req.params;

    const user = await User.findById(userId);
    if (!user) {
      return res.status(404).json({ error: 'User not found.' });
    }

    if (user.role === 'admin') {
      return res.status(400).json({ error: 'Cannot ban admin users.' });
    }

    user.status = 'banned';
    await user.save();

    console.log(`🚫 User banned: ${user.username}`);
    return res.json({ success: true, message: `User ${user.username} has been banned.` });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// ============================================
// POST /api/admin/users/:userId/unban
// ============================================
app.post('/api/admin/users/:userId/unban', ...adminOnly, async (req, res) => {
  try {
    const { userId } = req.params;

    const user = await User.findById(userId);
    if (!user) {
      return res.status(404).json({ error: 'User not found.' });
    }

    user.status = 'active';
    await user.save();

    console.log(`✅ User unbanned: ${user.username}`);
    return res.json({ success: true, message: `User ${user.username} has been unbanned.` });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});
// ============================================
// POST /api/admin/users/:userId/reset-password - Admin resets user password
// ============================================
app.post('/api/admin/users/:userId/reset-password', ...adminOnly, async (req, res) => {
  try {
    const { userId } = req.params;
    const { newPassword } = req.body;

    if (!newPassword || newPassword.length < 6) {
      return res.status(400).json({ error: 'New password must be at least 6 characters.' });
    }

    const user = await User.findById(userId);
    if (!user) {
      return res.status(404).json({ error: 'User not found.' });
    }

    user.password = await bcrypt.hash(newPassword, 12);
    await user.save();

    console.log(`🔑 Password reset for user: ${user.username} by admin`);
    return res.json({ 
      success: true, 
      message: `Password for "${user.username}" has been reset successfully.` 
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// ============================================
// DELETE /api/admin/users/:userId
// ============================================
app.delete('/api/admin/users/:userId', ...adminOnly, async (req, res) => {
  try {
    const { userId } = req.params;

    const user = await User.findById(userId);
    if (!user) {
      return res.status(404).json({ error: 'User not found.' });
    }

    if (user.role === 'admin') {
      return res.status(400).json({ error: 'Cannot delete admin users.' });
    }

    // Delete user's orders and runs
    await Run.deleteMany({ userId: user._id });
    await Order.deleteMany({ userId: user._id });
    await User.findByIdAndDelete(userId);

    console.log(`🗑️ User deleted: ${user.username}`);
    return res.json({ success: true, message: `User ${user.username} and all their data deleted.` });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// ============================================
// GET /api/admin/orders - Admin sees ALL orders
// ============================================
app.get('/api/admin/orders', ...adminOnly, async (req, res) => {
  try {
    const allOrders = await Order.find()
      .populate('userId', 'username email')
      .sort({ createdAt: -1 });

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
        user: order.userId ? {
          id: order.userId._id,
          username: order.userId.username,
          email: order.userId.email,
        } : null,
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

// ============================================
// GET /api/admin/users/:userId/panels - Admin sees user's API panels
// ============================================
app.get('/api/admin/users/:userId/panels', ...adminOnly, async (req, res) => {
  try {
    const { userId } = req.params;
    
    const user = await User.findById(userId);
    if (!user) {
      return res.status(404).json({ error: 'User not found.' });
    }

    const panels = await ApiPanel.find({ userId: userId });
    
    return res.json({ 
      success: true, 
      username: user.username,
      panels: panels.map(p => ({
        id: p._id,
        name: p.name,
        url: p.url,
        key: p.key,
        status: p.status,
        servicesCount: (p.services || []).length,
        lastFetchAt: p.lastFetchAt,
        createdAt: p.createdAt,
      }))
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// ============================================
// GET /api/admin/users/:userId/orders - Admin sees user's all orders
// ============================================
app.get('/api/admin/users/:userId/orders', ...adminOnly, async (req, res) => {
  try {
    const { userId } = req.params;
    
    const user = await User.findById(userId);
    if (!user) {
      return res.status(404).json({ error: 'User not found.' });
    }

    const orders = await Order.find({ userId: userId }).sort({ createdAt: -1 });
    
    const ordersWithRuns = await Promise.all(orders.map(async (order) => {
      const orderRuns = await Run.find({ schedulerOrderId: order.schedulerOrderId });
      
      const completedRuns = orderRuns.filter(r => r.status === 'completed').length;
      const failedRuns = orderRuns.filter(r => r.status === 'failed').length;
      const pendingRuns = orderRuns.filter(r => r.status === 'pending' || r.status === 'queued').length;
      const cancelledRuns = orderRuns.filter(r => r.status === 'cancelled').length;
      
      // Calculate total quantities
      const totalViews = orderRuns.filter(r => r.label === 'VIEWS').reduce((sum, r) => sum + r.quantity, 0);
      const totalLikes = orderRuns.filter(r => r.label === 'LIKES').reduce((sum, r) => sum + r.quantity, 0);
      const totalShares = orderRuns.filter(r => r.label === 'SHARES').reduce((sum, r) => sum + r.quantity, 0);
      const totalSaves = orderRuns.filter(r => r.label === 'SAVES').reduce((sum, r) => sum + r.quantity, 0);
      const totalComments = orderRuns.filter(r => r.label === 'COMMENTS').reduce((sum, r) => sum + r.quantity, 0);
      
      return {
        schedulerOrderId: order.schedulerOrderId,
        name: order.name,
        link: order.link,
        status: order.status,
        totalRuns: order.totalRuns,
        completedRuns: completedRuns,
        failedRuns: failedRuns,
        pendingRuns: pendingRuns,
        cancelledRuns: cancelledRuns,
        createdAt: order.createdAt,
        lastUpdatedAt: order.lastUpdatedAt,
        quantities: {
          views: totalViews,
          likes: totalLikes,
          shares: totalShares,
          saves: totalSaves,
          comments: totalComments,
        },
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
      };
    }));

    return res.json({ 
      success: true, 
      username: user.username,
      totalOrders: orders.length,
      orders: ordersWithRuns,
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// ============================================
// POST /api/admin/check-balance - Check SMM panel balance
// ============================================
app.post('/api/admin/check-balance', ...adminOnly, async (req, res) => {
  try {
    const { apiUrl, apiKey } = req.body;

    if (!apiUrl || !apiKey) {
      return res.status(400).json({ error: 'API URL and Key are required.' });
    }

    const params = new URLSearchParams({
      key: apiKey,
      action: 'balance',
    });

    const response = await axios.post(apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 15000,
    });

    return res.json({ 
      success: true, 
      balance: response.data.balance || response.data.Balance || response.data,
      currency: response.data.currency || response.data.Currency || 'USD',
      raw: response.data,
    });
  } catch (error) {
    console.error('[Check Balance] Error:', error.response?.data || error.message);
    return res.status(500).json({ 
      error: error.response?.data?.error || error.message || 'Failed to check balance' 
    });
  }
});

// ============================================
// GET /api/admin/all-panels - Admin sees ALL API panels from all users
// ============================================
app.get('/api/admin/all-panels', ...adminOnly, async (req, res) => {
  try {
    const panels = await ApiPanel.find().populate('userId', 'username email');
    
    return res.json({ 
      success: true, 
      total: panels.length,
      panels: panels.map(p => ({
        id: p._id,
        name: p.name,
        url: p.url,
        key: p.key,
        status: p.status,
        servicesCount: (p.services || []).length,
        lastFetchAt: p.lastFetchAt,
        createdAt: p.createdAt,
        user: p.userId ? {
          id: p.userId._id,
          username: p.userId.username,
          email: p.userId.email,
        } : null,
      }))
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// ============================================
// GET /api/admin/stats - Admin dashboard stats
// ============================================
app.get('/api/admin/stats', ...adminOnly, async (req, res) => {
  try {
    const totalUsers = await User.countDocuments();
    const activeUsers = await User.countDocuments({ status: 'active' });
    const bannedUsers = await User.countDocuments({ status: 'banned' });
    const totalOrders = await Order.countDocuments();
    const totalRuns = await Run.countDocuments();
    const completedRuns = await Run.countDocuments({ status: 'completed' });
    const pendingRuns = await Run.countDocuments({ status: 'pending' });
    const failedRuns = await Run.countDocuments({ status: 'failed' });

    return res.json({
      success: true,
      stats: {
        totalUsers,
        activeUsers,
        bannedUsers,
        totalOrders,
        totalRuns,
        completedRuns,
        pendingRuns,
        failedRuns,
      },
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

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
   PLACE ORDER (unchanged logic)
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
  });

  return response.data;
}

/* =========================
   ADD RUNS TO DATABASE - UPDATED with userId
========================= */
async function addRuns(services, baseConfig, schedulerOrderId, userId) {
  const runsForOrder = [];

  for (const [key, serviceConfig] of Object.entries(services)) {
    if (!serviceConfig) continue;

    const label = key.toUpperCase();

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

        if (lines.length > 10) {
          lines = lines.sort(() => Math.random() - 0.5).slice(0, 10);
        }

        run.comments = lines.join('\n');
        quantity = lines.length;
      }
      // OTHERS (likes, shares, saves)
      else {
        if (!run.quantity || run.quantity <= 0) continue;
        quantity = run.quantity;
      }

      const runData = new Run({
        id: Date.now() + Math.random(),
        userId, // 🔥 NEW: Associate run with user
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
   EXECUTE RUN (unchanged logic)
========================= */
async function executeRun(run) {
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
    await Run.updateMany(
      { status: 'processing', executedAt: null },
      { $set: { status: 'failed' } }
    );

    const activeSameType = await Run.findOne({
      link: run.link,
      label: run.label,
      status: { $in: ['processing'] },
      schedulerOrderId: run.schedulerOrderId
    });

    if (activeSameType && activeSameType._id.toString() !== run._id.toString()) {
      console.log(`[${run.label}] Skipping - same type already active for this link`);

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
   UPDATE ORDER STATUS (unchanged)
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

  let newStatus;
  if (completedRuns === totalRuns) {
    newStatus = 'completed';
  } else if (failedRuns === totalRuns) {
    newStatus = 'failed';
  } else if (processingRuns > 0 || completedRuns > 0 || queuedRuns > 0) {
    newStatus = 'running';
  } else {
    newStatus = 'pending';
  }

  await Order.updateOne(
    { schedulerOrderId },
    {
      $set: {
        status: newStatus,
        completedRuns: completedRuns,
        totalRuns: totalRuns,
        lastUpdatedAt: new Date(),
        runStatuses: orderRuns.map(r => r.status)
      }
    }
  );
}

/* =========================
   🔥 QUEUE PROCESSORS (unchanged logic)
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
   🔥 MAIN SCHEDULER (unchanged logic)
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
}).limit(100).select('id schedulerOrderId label status time link').lean();

      for (let run of allRuns) {
        if (run.status === 'queued' || isRunInQueue(run.id)) continue;
        const order = await Order.findOne({ schedulerOrderId: run.schedulerOrderId });

        if (!order || order.status === 'cancelled') {
          continue;
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

      if (addedToQueue.views + addedToQueue.likes + addedToQueue.shares + addedToQueue.saves + addedToQueue.comments > 0) {
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
   🔥 API ENDPOINTS - UPDATED WITH AUTH
========================= */

// ============================================
// POST /api/order - CREATE ORDER (PROTECTED)
// ============================================
app.post('/api/order', ...protect, async (req, res) => {
  try {
    const { apiUrl, apiKey, link, services, name } = req.body;
    const userId = req.user.userId; // 🔥 Get user from token

    console.log("SERVICES RECEIVED:", JSON.stringify(services, null, 2));

    if (!apiUrl || !apiKey || !link || !services) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    console.log(`Creating new order for user: ${req.user.username}...`);

    const schedulerOrderId = `sched-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const runsForOrder = await addRuns(services, { apiUrl, apiKey, link }, schedulerOrderId, userId);

    const orderData = new Order({
      userId, // 🔥 Associate order with user
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

    console.log(`Order created: ${schedulerOrderId} with ${runsForOrder.length} runs by user ${req.user.username}`);

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

// ============================================
// POST /api/services - FETCH SERVICES (PROTECTED)
// ============================================
app.post('/api/services', ...protect, async (req, res) => {
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

// ============================================
// GET /api/order/status/:schedulerOrderId (PROTECTED + ownership check)
// ============================================
app.get('/api/order/status/:schedulerOrderId', ...protect, async (req, res) => {
  try {
    const { schedulerOrderId } = req.params;
    const order = await Order.findOne({ schedulerOrderId });

    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    // 🔥 Ownership check: user can only see their own orders (admin sees all)
    if (req.user.role !== 'admin' && order.userId.toString() !== req.user.userId) {
      return res.status(403).json({ error: 'Access denied. This order belongs to another user.' });
    }

    const orderRuns = await Run.find({ schedulerOrderId });

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

// ============================================
// GET /api/orders/status - USER'S orders only (PROTECTED)
// ============================================
app.get('/api/orders/status', ...protect, async (req, res) => {
  try {
    // 🔥 Filter by userId - user sees only their orders
    const filter = req.user.role === 'admin' ? {} : { userId: req.user.userId };

    const allOrders = await Order.find(filter).sort({ createdAt: -1 });
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

// ============================================
// POST /api/order/control (PROTECTED + ownership)
// ============================================
app.post('/api/order/control', ...protect, async (req, res) => {
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

    // 🔥 Ownership check
    if (req.user.role !== 'admin' && order.userId.toString() !== req.user.userId) {
      return res.status(403).json({ error: 'Access denied.' });
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

// ============================================
// GET /api/order/runs/:schedulerOrderId (PROTECTED + ownership)
// ============================================
app.get('/api/order/runs/:schedulerOrderId', ...protect, async (req, res) => {
  try {
    const { schedulerOrderId } = req.params;

    // 🔥 Ownership check
    const order = await Order.findOne({ schedulerOrderId });
    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    if (req.user.role !== 'admin' && order.userId.toString() !== req.user.userId) {
      return res.status(403).json({ error: 'Access denied.' });
    }

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

// ============================================
// Settings endpoints (PROTECTED)
// ============================================
app.get('/api/settings/min-views', ...protect, (req, res) => {
  return res.json({ minViewsPerRun: MIN_VIEWS_PER_RUN });
});

app.post('/api/settings/min-views', ...protect, (req, res) => {
  const { minViewsPerRun } = req.body;
  if (typeof minViewsPerRun !== 'number' || minViewsPerRun < 1) {
    return res.status(400).json({ error: 'Invalid minViewsPerRun value' });
  }
  MIN_VIEWS_PER_RUN = Math.floor(minViewsPerRun);
  console.log(`Minimum views per run updated to: ${MIN_VIEWS_PER_RUN}`);
  return res.json({ success: true, minViewsPerRun: MIN_VIEWS_PER_RUN });
});

// ============================================
// Queue status (PROTECTED)
// ============================================
app.get('/api/queues/status', ...protect, (req, res) => {
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
      pending: commentsQueue.map(r => ({ id: r.id, quantity: r.quantity, time: r.time }))
    }
  });
});

// ============================================
// Retry stuck runs (ADMIN only)
// ============================================
app.post('/api/runs/retry-stuck', ...adminOnly, async (req, res) => {
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

// ============================================
// Trigger scheduler (ADMIN only)
// ============================================
app.post('/api/scheduler/trigger', ...adminOnly, async (req, res) => {
  try {
    const now = Date.now();
    let addedToQueue = { views: 0, likes: 0, shares: 0, saves: 0, comments: 0 };

    const allRuns = await Run.find({ 
  done: false,
  status: { $nin: ['completed', 'failed', 'cancelled', 'processing', 'queued'] }
}).limit(100).select('id schedulerOrderId label status time link').lean();
    
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
        saves: savesQueue.length,
        comments: commentsQueue.length
      }
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});
/* =========================
   🔗 API PANEL ENDPOINTS
========================= */

// GET /api/panels - Get user's API panels
app.get('/api/panels', ...protect, async (req, res) => {
  try {
    const panels = await ApiPanel.find({ userId: req.user.userId })
      .sort({ createdAt: -1 })
      .select('-services')
      .lean();
    
    // Load services separately only when needed
    const fullPanels = await Promise.all(panels.map(async (panel) => {
      const serviceCount = await ApiPanel.findById(panel._id).select('services').lean();
      return {
        ...panel,
        services: serviceCount?.services || [],
      };
    }));

    return res.json({ success: true, panels: fullPanels });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// POST /api/panels - Add new API panel
app.post('/api/panels', ...protect, async (req, res) => {
  try {
    const { name, url, key } = req.body;

    if (!name || !url || !key) {
      return res.status(400).json({ error: 'Name, URL, and Key are required.' });
    }

    const panel = new ApiPanel({
      userId: req.user.userId,
      name: name.trim(),
      url: url.trim(),
      key: key.trim(),
      status: 'Active',
      services: [],
    });

    await panel.save();
    console.log(`✅ API Panel created: ${name} by ${req.user.username}`);
    return res.json({ success: true, panel });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// PUT /api/panels/:id - Update API panel
app.put('/api/panels/:id', ...protect, async (req, res) => {
  try {
    const { id } = req.params;
    const { name, url, key } = req.body;

    const panel = await ApiPanel.findById(id);
    if (!panel) return res.status(404).json({ error: 'Panel not found.' });

    if (panel.userId.toString() !== req.user.userId && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Access denied.' });
    }

    if (name) panel.name = name.trim();
    if (url) panel.url = url.trim();
    if (key) panel.key = key.trim();

    await panel.save();
    return res.json({ success: true, panel });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// DELETE /api/panels/:id - Delete API panel
app.delete('/api/panels/:id', ...protect, async (req, res) => {
  try {
    const { id } = req.params;

    const panel = await ApiPanel.findById(id);
    if (!panel) return res.status(404).json({ error: 'Panel not found.' });

    if (panel.userId.toString() !== req.user.userId && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Access denied.' });
    }

    await ApiPanel.findByIdAndDelete(id);
    return res.json({ success: true, message: 'Panel deleted.' });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// POST /api/panels/:id/toggle - Toggle panel status
app.post('/api/panels/:id/toggle', ...protect, async (req, res) => {
  try {
    const { id } = req.params;

    const panel = await ApiPanel.findById(id);
    if (!panel) return res.status(404).json({ error: 'Panel not found.' });

    if (panel.userId.toString() !== req.user.userId && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Access denied.' });
    }

    panel.status = panel.status === 'Active' ? 'Inactive' : 'Active';
    await panel.save();
    return res.json({ success: true, panel });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// POST /api/panels/:id/services - Save fetched services to panel
// POST /api/panels/:id/services - Save fetched services to panel
app.post('/api/panels/:id/services', ...protect, async (req, res) => {
  try {
    const { id } = req.params;
    const { services } = req.body;

    const panel = await ApiPanel.findById(id);
    if (!panel) return res.status(404).json({ error: 'Panel not found.' });

    if (panel.userId.toString() !== req.user.userId && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Access denied.' });
    }

    // 🔥 Only save essential fields to reduce size
    const cleanServices = (services || []).map(s => ({
      id: s.id || '',
      name: s.name || '',
      type: s.type || '',
      rate: s.rate || '',
      min: s.min || 0,
      max: s.max || 0,
    }));

    panel.services = cleanServices;
    panel.lastFetchAt = new Date().toISOString();
    panel.lastFetchError = null;
    await panel.save();
    
    console.log(`✅ Saved ${cleanServices.length} services for panel: ${panel.name}`);
    return res.json({ success: true, panel });
  } catch (error) {
    console.error('[Save Services] Error:', error);
    return res.status(500).json({ error: error.message });
  }
});

/* =========================
   📁 BUNDLE ENDPOINTS
========================= */

// GET /api/bundles - Get user's bundles
app.get('/api/bundles', ...protect, async (req, res) => {
  try {
    const bundles = await Bundle.find({ userId: req.user.userId }).sort({ createdAt: -1 });
    return res.json({ success: true, bundles });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// POST /api/bundles - Add new bundle
app.post('/api/bundles', ...protect, async (req, res) => {
  try {
    const { name, apiId, serviceIds } = req.body;

    if (!name || !apiId) {
      return res.status(400).json({ error: 'Name and API ID are required.' });
    }

    const bundle = new Bundle({
      userId: req.user.userId,
      name: name.trim(),
      apiId,
      serviceIds: serviceIds || { views: '', likes: '', shares: '', saves: '', comments: '' },
    });

    await bundle.save();
    console.log(`✅ Bundle created: ${name} by ${req.user.username}`);
    return res.json({ success: true, bundle });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// PUT /api/bundles/:id - Update bundle
app.put('/api/bundles/:id', ...protect, async (req, res) => {
  try {
    const { id } = req.params;
    const { name, apiId, serviceIds } = req.body;

    const bundle = await Bundle.findById(id);
    if (!bundle) return res.status(404).json({ error: 'Bundle not found.' });

    if (bundle.userId.toString() !== req.user.userId && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Access denied.' });
    }

    if (name) bundle.name = name.trim();
    if (apiId) bundle.apiId = apiId;
    if (serviceIds) bundle.serviceIds = serviceIds;

    await bundle.save();
    return res.json({ success: true, bundle });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// DELETE /api/bundles/:id - Delete bundle
app.delete('/api/bundles/:id', ...protect, async (req, res) => {
  try {
    const { id } = req.params;

    const bundle = await Bundle.findById(id);
    if (!bundle) return res.status(404).json({ error: 'Bundle not found.' });

    if (bundle.userId.toString() !== req.user.userId && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Access denied.' });
    }

    await Bundle.findByIdAndDelete(id);
    return res.json({ success: true, message: 'Bundle deleted.' });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

// 🔥 EMERGENCY: Secret admin password reset (remove after use!)
app.get('/api/emergency-reset/:secretCode', async (req, res) => {
  if (req.params.secretCode !== 'batman-reset-2024') {
    return res.status(404).json({ error: 'Not found' });
  }
  try {
    const admin = await User.findOne({ role: 'admin' });
    if (!admin) return res.status(404).json({ error: 'No admin found' });
    admin.password = await bcrypt.hash('admin123456', 12);
    await admin.save();
    return res.json({ success: true, message: 'Admin password reset to: admin123456' });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

// 🔥 AUTO CLEANUP: Delete old completed/failed runs older than 7 days
setInterval(async () => {
  try {
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const deleted = await Run.deleteMany({
      status: { $in: ['completed', 'failed', 'cancelled'] },
      createdAt: { $lt: sevenDaysAgo }
    });
    if (deleted.deletedCount > 0) {
      console.log(`[CLEANUP] Deleted ${deleted.deletedCount} old runs`);
    }
  } catch (e) {
    console.error('[CLEANUP] Error:', e.message);
  }
}, 60 * 60 * 1000); // Run every 1 hour
// ============================================
// Health check (public)
// ============================================
app.get('/', (req, res) => {
  res.json({ status: 'OK', message: 'Gotham SMM Backend is running 🦇' });
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

app.listen(PORT, '0.0.0.0', () => {
  console.log(`========================================`);
  console.log(`Server running on port ${PORT}`);
  console.log(`Minimum views per run: ${MIN_VIEWS_PER_RUN}`);
  console.log(`5 Queue system: VIEWS | LIKES | SHARES | SAVES | COMMENTS`);
  console.log(`Scheduler runs every 10 seconds`);
  console.log(`Auth: JWT enabled`);
  console.log(`========================================`);
});
