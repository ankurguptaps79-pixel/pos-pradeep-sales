/* Firebase Cloud Sync for Pradeep Sales POS
   – Runs in background, IndexedDB stays primary
   – Syncs products, customers, sales, invoiceItems, loyalty, settings to Firestore
   – Uses a single device ID so multi-device doesn't duplicate
*/
'use strict';

const FIRE_CFG = {
  apiKey: "AIzaSyB1Vra5dN8MKrOkT_TigjagM4LFlXhPfTo",
  authDomain: "pradeep-pos.firebaseapp.com",
  projectId: "pradeep-pos",
  storageBucket: "pradeep-pos.firebasestorage.app",
  messagingSenderId: "130205960665",
  appId: "1:130205960665:web:52f867ef344704690eb1c8"
};

/* ================= globals ================= */
let fireDb = null, syncReady = false, syncPaused = false;
let deviceId = localStorage.getItem('ps_device_id');
if (!deviceId) { deviceId = 'dev_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8); localStorage.setItem('ps_device_id', deviceId); }

const SYNC_STORES = ['products', 'customers', 'sales', 'invoiceItems', 'loyalty'];
const syncQueue = [];        // { action:'put'|'del', store, id, data }
let flushing = false;

/* ================= status ================= */
let lastSync = +localStorage.getItem('ps_last_sync') || 0;
let syncStatus = 'idle';     // idle | syncing | error | offline
const syncListeners = [];
function onSyncStatus(fn) { syncListeners.push(fn); }
function setSyncStatus(s) { syncStatus = s; syncListeners.forEach(fn => fn(s)); }
function getLastSync() { return lastSync; }

/* ================= init ================= */
async function initFirebase() {
  try {
    if (typeof firebase === 'undefined') { console.warn('Firebase SDK not loaded'); return; }
    firebase.initializeApp(FIRE_CFG);
    fireDb = firebase.firestore();
    // Enable offline persistence
    try { await fireDb.enablePersistence({ synchronizeTabs: true }); } catch (e) {
      if (e.code === 'unimplemented') console.log('Firestore persistence not available in this browser');
    }
    syncReady = true;
    setSyncStatus('idle');
    console.log('Firebase sync ready');
  } catch (err) {
    console.error('Firebase init failed:', err);
    setSyncStatus('error');
  }
}

/* ================= queue & flush ================= */
function queueSync(action, store, id, data) {
  if (!syncReady || syncPaused) return;
  syncQueue.push({ action, store, id, data, ts: Date.now() });
  scheduleFlush();
}

let flushTimer;
function scheduleFlush() { clearTimeout(flushTimer); flushTimer = setTimeout(flushQueue, 500); }

async function flushQueue() {
  if (flushing || !syncReady || !syncQueue.length) return;
  flushing = true;
  setSyncStatus('syncing');
  try {
    const batch = fireDb.batch();
    let count = 0;
    while (syncQueue.length && count < 400) {   // Firestore batch max is 500
      const job = syncQueue.shift();
      const docRef = fireDb.collection(job.store).doc(String(job.id));
      if (job.action === 'put') {
        const clean = sanitize(job.data);
        clean._deviceId = deviceId;
        clean._updatedAt = firebase.firestore.FieldValue.serverTimestamp();
        batch.set(docRef, clean, { merge: true });
      } else {
        batch.delete(docRef);
      }
      count++;
    }
    if (count) {
      await batch.commit();
      lastSync = Date.now();
      localStorage.setItem('ps_last_sync', lastSync);
    }
    setSyncStatus('idle');
  } catch (err) {
    console.error('Sync flush error:', err);
    setSyncStatus('error');
    // items already shifted — that's OK, they'll be re-synced on full sync
  }
  flushing = false;
  if (syncQueue.length) scheduleFlush();
}

/* Strip functions and ensure no undefined values (Firestore rejects them) */
function sanitize(obj) {
  const o = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v === undefined || typeof v === 'function') continue;
    if (v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Date)) o[k] = sanitize(v);
    else o[k] = v;
  }
  return o;
}

/* ================= full sync (upload all local → cloud) ================= */
async function fullUpload() {
  if (!syncReady || !db) return;
  setSyncStatus('syncing');
  try {
    for (const store of SYNC_STORES) {
      const all = await dbAll(store);
      // batch in groups of 400
      for (let i = 0; i < all.length; i += 400) {
        const batch = fireDb.batch();
        const chunk = all.slice(i, i + 400);
        for (const item of chunk) {
          const docRef = fireDb.collection(store).doc(String(item.id));
          const clean = sanitize(item);
          clean._deviceId = deviceId;
          clean._updatedAt = firebase.firestore.FieldValue.serverTimestamp();
          batch.set(docRef, clean, { merge: true });
        }
        await batch.commit();
      }
    }
    // sync settings too
    const settings = await dbAll('settings');
    if (settings.length) {
      const batch = fireDb.batch();
      for (const s of settings) {
        if (s.key === 'draft') continue; // don't sync drafts
        const docRef = fireDb.collection('settings').doc(s.key);
        batch.set(docRef, { key: s.key, value: typeof s.value === 'object' ? JSON.stringify(s.value) : s.value, _deviceId: deviceId, _updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true });
      }
      await batch.commit();
    }
    lastSync = Date.now();
    localStorage.setItem('ps_last_sync', lastSync);
    setSyncStatus('idle');
    return true;
  } catch (err) {
    console.error('Full upload error:', err);
    setSyncStatus('error');
    return false;
  }
}

/* ================= full download (cloud → local) ================= */
async function fullDownload() {
  if (!syncReady || !db) return;
  setSyncStatus('syncing');
  try {
    for (const store of SYNC_STORES) {
      const snap = await fireDb.collection(store).get();
      if (snap.empty) continue;
      for (const doc of snap.docs) {
        const data = doc.data();
        delete data._deviceId; delete data._updatedAt;
        // restore numeric id
        if (data.id && typeof data.id === 'string') data.id = parseInt(data.id) || data.id;
        await dbPut(store, data);
      }
    }
    // reload in-memory state
    await Promise.all([loadProducts(), loadCustomers(), loadLoyalty()]);
    lastSync = Date.now();
    localStorage.setItem('ps_last_sync', lastSync);
    setSyncStatus('idle');
    return true;
  } catch (err) {
    console.error('Full download error:', err);
    setSyncStatus('error');
    return false;
  }
}

/* ================= sync settings ================= */
function syncSetting(key, value) {
  if (!syncReady || syncPaused || key === 'draft') return;
  const docRef = fireDb.collection('settings').doc(key);
  const v = typeof value === 'object' ? JSON.stringify(value) : value;
  docRef.set({ key, value: v, _deviceId: deviceId, _updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true }).catch(e => console.warn('Setting sync fail:', e));
}

/* ================= hook into app ================= */
// These are called from app.js after local dbPut / dbDel
function onLocalPut(store, data) { if (data && data.id != null) queueSync('put', store, data.id, data); }
function onLocalDel(store, id) { queueSync('del', store, id, null); }
