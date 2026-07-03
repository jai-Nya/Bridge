/* ==========================================================================
   app.js
   Ties together: Firestore signaling, WebRTC peer connection, the folder
   share feature, and the UI. Only tiny JSON messages (SDP, ICE candidates,
   presence heartbeats) ever touch Firestore — file bytes only ever flow
   through the direct RTCDataChannel.
   ========================================================================== */

const LS_KEY = 'bridge.settings.v1';
const HEARTBEAT_MS = 15000;
const PEER_STALE_MS = 40000;

const STUN_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
];

// ---------------------------------------------------------------------------
// DOM refs
// ---------------------------------------------------------------------------
const $ = (id) => document.getElementById(id);

const setupScreen = $('setupScreen');
const mainScreen = $('mainScreen');
const setupError = $('setupError');

const els = {
  deviceLabel: $('deviceLabel'),
  settingsBtn: $('settingsBtn'),
  pairKey: $('pairKey'),
  deviceName: $('deviceName'),
  fbConfig: $('fbConfig'),
  turnUrl: $('turnUrl'),
  turnUser: $('turnUser'),
  turnPass: $('turnPass'),
  saveConfigBtn: $('saveConfigBtn'),
  nodeSelf: $('nodeSelf'),
  nodeSelfLabel: $('nodeSelfLabel'),
  nodePeer: $('nodePeer'),
  nodePeerLabel: $('nodePeerLabel'),
  bridgeLink: document.querySelector('.bridge-link'),
  bridgeParticle: $('bridgeParticle'),
  statusText: $('statusText'),
  statusMeta: $('statusMeta'),
  dropzone: $('dropzone'),
  filePicker: $('filePicker'),
  folderPanel: $('folderPanel'),
  chooseFolderBtn: $('chooseFolderBtn'),
  folderStatus: $('folderStatus'),
  peerFolderPanel: $('peerFolderPanel'),
  peerFileList: $('peerFileList'),
  peerFolderEmpty: $('peerFolderEmpty'),
  refreshPeerFolderBtn: $('refreshPeerFolderBtn'),
  transferList: $('transferList'),
  transferEmpty: $('transferEmpty'),
};

const transferItemTpl = $('transferItemTemplate');
const peerFileItemTpl = $('peerFileItemTemplate');

// ---------------------------------------------------------------------------
// Settings persistence
// ---------------------------------------------------------------------------
function loadSettings() {
  try { return JSON.parse(localStorage.getItem(LS_KEY) || 'null'); } catch { return null; }
}
function saveSettings(s) { localStorage.setItem(LS_KEY, JSON.stringify(s)); }

function getOrCreateDeviceId() {
  let id = localStorage.getItem('bridge.deviceId');
  if (!id) {
    id = crypto.randomUUID ? crypto.randomUUID() : (Date.now().toString(36) + Math.random().toString(36).slice(2));
    localStorage.setItem('bridge.deviceId', id);
  }
  return id;
}

async function sha256Hex(str) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(str));
  return Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, '0')).join('');
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------
let settings = loadSettings();
const deviceId = getOrCreateDeviceId();

let db = null;
let pairHash = null;
let pc = null;              // RTCPeerConnection
let transferChannel = null; // TransferChannel wrapping the open data channel
let myEpoch = null;         // epoch this device most recently initiated
let watchedEpoch = null;    // epoch this device is currently answering/waiting on
let unsubPointer = null;
let unsubPresence = null;
let unsubSessionDoc = null;
let unsubOfferCands = null;
let unsubAnswerCands = null;
let peerOnline = false;
let peerDeviceId = null;
let peerName = 'Peer device';
let heartbeatTimer = null;
let staleCheckTimer = null;
let retryTimer = null;

let sharedDirHandle = null;   // this device's exposed folder (File System Access API)
let sharedFileMap = new Map(); // name -> FileSystemFileHandle

const transferRows = new Map(); // id -> { el, fillEl, metaEl }

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
init();

function init() {
  if (settings && settings.pairKey && settings.fbConfig) {
    startApp();
  } else {
    showSetup();
  }
  els.settingsBtn.addEventListener('click', () => {
    prefillSetupFromSettings();
    showSetup();
  });
  els.saveConfigBtn.addEventListener('click', onSaveConfig);
}

function prefillSetupFromSettings() {
  if (!settings) return;
  els.pairKey.value = settings.pairKey || '';
  els.deviceName.value = settings.deviceName || '';
  els.fbConfig.value = settings.fbConfig ? JSON.stringify(settings.fbConfig, null, 2) : '';
  els.turnUrl.value = settings.turn?.url || '';
  els.turnUser.value = settings.turn?.username || '';
  els.turnPass.value = settings.turn?.password || '';
}

function showSetup() {
  setupScreen.hidden = false;
  mainScreen.hidden = true;
}

function showMain() {
  setupScreen.hidden = true;
  mainScreen.hidden = false;
}

async function onSaveConfig() {
  setupError.hidden = true;
  const pairKey = els.pairKey.value.trim();
  const deviceName = els.deviceName.value.trim() || (/Android|iPhone/.test(navigator.userAgent) ? 'Phone' : 'Computer');
  let fbConfig;
  try {
    fbConfig = JSON.parse(els.fbConfig.value);
    if (!fbConfig.apiKey || !fbConfig.projectId) throw new Error('missing apiKey/projectId');
  } catch (err) {
    setupError.textContent = 'That Firebase config doesn\u2019t look valid JSON. Paste the full object from the Firebase console.';
    setupError.hidden = false;
    return;
  }
  if (!pairKey || pairKey.length < 6) {
    setupError.textContent = 'Use a pair key that\u2019s at least 6 characters — longer and more random is safer.';
    setupError.hidden = false;
    return;
  }

  const turn = els.turnUrl.value.trim()
    ? { url: els.turnUrl.value.trim(), username: els.turnUser.value.trim(), password: els.turnPass.value }
    : null;

  settings = { pairKey, deviceName, fbConfig, turn };
  saveSettings(settings);
  teardownConnection();
  startApp();
}

// ---------------------------------------------------------------------------
// Startup: Firebase + presence + signaling watchers
// ---------------------------------------------------------------------------
async function startApp() {
  showMain();
  els.nodeSelfLabel.textContent = settings.deviceName;
  els.deviceLabel.textContent = settings.deviceName;
  setStatus('Connecting to signaling…', '');

  try {
    if (!firebase.apps.length) firebase.initializeApp(settings.fbConfig);
    db = firebase.firestore();
  } catch (err) {
    setStatus('Firebase config error — check Settings.', '');
    console.error(err);
    return;
  }

  pairHash = 'p_' + (await sha256Hex(settings.pairKey));

  wireFileUI();
  wireFolderUI();

  startPresence();
  watchPointer();
}

// Simple flat paths, easier to reason about than deep nesting:
//   pairs/{hash}/presence/{deviceId}
//   pairs/{hash}/signal/pointer
//   pairs/{hash}/signal/session_{epoch}
//   pairs/{hash}/signal/session_{epoch}/offerCandidates/*
//   pairs/{hash}/signal/session_{epoch}/answerCandidates/*
function pairDoc() { return db.collection('pairs').doc(pairHash); }
function presenceCol() { return pairDoc().collection('presence'); }
function pointerDoc() { return pairDoc().collection('signal').doc('pointer'); }
function sessionDoc(epoch) { return pairDoc().collection('signal').doc('session_' + epoch); }

// ---------------------------------------------------------------------------
// Presence (heartbeat + freshness watch)
// ---------------------------------------------------------------------------
function startPresence() {
  const myPresence = presenceCol().doc(deviceId);
  const beat = () => myPresence.set({
    name: settings.deviceName,
    updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
  }).catch((err) => console.warn('heartbeat failed', err));

  beat();
  heartbeatTimer = setInterval(beat, HEARTBEAT_MS);

  unsubPresence = presenceCol().onSnapshot((snap) => {
    let foundPeer = null;
    const now = Date.now();
    snap.forEach((doc) => {
      if (doc.id === deviceId) return;
      const data = doc.data();
      const ts = data.updatedAt?.toMillis ? data.updatedAt.toMillis() : now;
      if (now - ts < PEER_STALE_MS) {
        foundPeer = { id: doc.id, name: data.name || 'Peer device' };
      }
    });
    handlePeerPresenceChange(foundPeer);
  }, (err) => {
    console.error('presence watch error', err);
    setStatus('Signaling error — check Firestore rules.', '');
  });

  staleCheckTimer = setInterval(() => {
    // Re-evaluate freshness even if no snapshot fired recently.
    if (peerOnline && peerDeviceId) {
      presenceCol().doc(peerDeviceId).get().then((doc) => {
        if (!doc.exists) return handlePeerPresenceChange(null);
        const ts = doc.data().updatedAt?.toMillis ? doc.data().updatedAt.toMillis() : Date.now();
        if (Date.now() - ts > PEER_STALE_MS) handlePeerPresenceChange(null);
      });
    }
  }, HEARTBEAT_MS);
}

function handlePeerPresenceChange(peer) {
  const wasOnline = peerOnline;
  peerOnline = !!peer;
  peerDeviceId = peer ? peer.id : peerDeviceId;
  peerName = peer ? peer.name : peerName;
  els.nodePeerLabel.textContent = peerOnline ? peerName : 'Peer device';
  els.nodePeer.classList.toggle('online', peerOnline && !!transferChannel);
  els.nodePeer.classList.toggle('waiting', peerOnline && !transferChannel);
  els.nodeSelf.classList.add('online');

  if (peerOnline && !wasOnline) {
    setStatus(`${peerName} is online — linking…`, '');
    maybeInitiate();
  } else if (!peerOnline && wasOnline) {
    setStatus('Peer went offline. Waiting…', '');
    teardownConnection(false);
  } else if (!peerOnline) {
    setStatus('Waiting for your other device…', '');
  }
}

// ---------------------------------------------------------------------------
// WebRTC handshake via Firestore signaling
// ---------------------------------------------------------------------------
function iceServers() {
  const list = [...STUN_SERVERS];
  if (settings.turn?.url) {
    list.push({ urls: settings.turn.url, username: settings.turn.username, credential: settings.turn.password });
  }
  return list;
}

function isInitiator() {
  // Deterministic tie-break so both sides agree on roles without extra messages.
  return peerDeviceId && deviceId < peerDeviceId;
}

function maybeInitiate() {
  if (!peerOnline || transferChannel) return;
  if (isInitiator()) startAsInitiator();
  // else: wait for the pointer doc to change (watchPointer handles it)
}

async function startAsInitiator() {
  const epoch = Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 6);
  myEpoch = epoch;

  pc = new RTCPeerConnection({ iceServers: iceServers() });
  wirePeerConnectionCommon();

  const channel = pc.createDataChannel('bridge', { ordered: true });
  wireDataChannel(channel);

  pc.onicecandidate = (e) => {
    if (e.candidate) sessionDoc(epoch).collection('offerCandidates').add(e.candidate.toJSON());
  };

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);

  await sessionDoc(epoch).set({ offer: { sdp: offer.sdp, type: offer.type }, by: deviceId });
  await pointerDoc().set({ epoch, initiator: deviceId, ts: firebase.firestore.FieldValue.serverTimestamp() });

  setStatus('Waiting for peer to answer…', '');

  unsubSessionDoc && unsubSessionDoc();
  unsubSessionDoc = sessionDoc(epoch).onSnapshot(async (doc) => {
    const data = doc.data();
    if (data?.answer && pc && !pc.currentRemoteDescription) {
      await pc.setRemoteDescription(new RTCSessionDescription(data.answer));
    }
  });

  unsubAnswerCands && unsubAnswerCands();
  unsubAnswerCands = sessionDoc(epoch).collection('answerCandidates').onSnapshot((snap) => {
    snap.docChanges().forEach((change) => {
      if (change.type === 'added' && pc) {
        pc.addIceCandidate(new RTCIceCandidate(change.doc.data())).catch(() => {});
      }
    });
  });
}

function watchPointer() {
  unsubPointer && unsubPointer();
  unsubPointer = pointerDoc().onSnapshot((doc) => {
    const data = doc.data();
    if (!data || data.initiator === deviceId) return; // ignore our own pointer writes
    if (data.epoch === watchedEpoch) return;           // already handling this epoch
    watchedEpoch = data.epoch;
    answerSession(data.epoch);
  });
}

async function answerSession(epoch) {
  teardownConnection(false);

  pc = new RTCPeerConnection({ iceServers: iceServers() });
  wirePeerConnectionCommon();

  pc.ondatachannel = (e) => wireDataChannel(e.channel);

  pc.onicecandidate = (e) => {
    if (e.candidate) sessionDoc(epoch).collection('answerCandidates').add(e.candidate.toJSON());
  };

  const snap = await sessionDoc(epoch).get();
  const data = snap.data();
  if (!data?.offer) return;

  await pc.setRemoteDescription(new RTCSessionDescription(data.offer));
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);
  await sessionDoc(epoch).set({ answer: { sdp: answer.sdp, type: answer.type } }, { merge: true });

  setStatus('Answering peer…', '');

  unsubOfferCands && unsubOfferCands();
  unsubOfferCands = sessionDoc(epoch).collection('offerCandidates').onSnapshot((snap2) => {
    snap2.docChanges().forEach((change) => {
      if (change.type === 'added' && pc) {
        pc.addIceCandidate(new RTCIceCandidate(change.doc.data())).catch(() => {});
      }
    });
  });
}

function wirePeerConnectionCommon() {
  pc.onconnectionstatechange = () => {
    if (!pc) return;
    if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
      setStatus('Link dropped. Retrying…', '');
      els.nodePeer.classList.remove('online');
      const wasInitiator = isInitiator();
      teardownConnection(false);
      clearTimeout(retryTimer);
      retryTimer = setTimeout(() => { if (peerOnline) maybeInitiate(); }, 3000);
    }
  };
}

function wireDataChannel(channel) {
  channel.addEventListener('open', () => {
    transferChannel = new TransferChannel(channel, {
      onControl: handleControlMessage,
      onProgress: handleProgress,
      onIncomingStart: handleIncomingStart,
      onComplete: handleComplete,
      onError: handleTransferError,
    });
    els.nodePeer.classList.add('online');
    els.nodePeer.classList.remove('waiting');
    els.bridgeLink.classList.add('linked');
    setStatus('Connected · direct P2P link', '🔒 encrypted');
    // Re-announce our folder listing (if any) on every fresh link.
    if (sharedFileMap.size) announceFolderListing();
    else if (sharedDirHandle) refreshSharedFolder();
  });
  channel.addEventListener('close', () => {
    transferChannel = null;
    els.nodePeer.classList.remove('online');
    els.bridgeLink.classList.remove('linked');
    els.peerFolderPanel.hidden = true;
    if (peerOnline) { els.nodePeer.classList.add('waiting'); setStatus('Link closed. Reconnecting…', ''); }
  });
}

function teardownConnection(clearWatch = true) {
  transferChannel = null;
  if (pc) { try { pc.close(); } catch {} pc = null; }
  unsubSessionDoc && unsubSessionDoc(); unsubSessionDoc = null;
  unsubOfferCands && unsubOfferCands(); unsubOfferCands = null;
  unsubAnswerCands && unsubAnswerCands(); unsubAnswerCands = null;
  els.nodePeer.classList.remove('online');
  els.bridgeLink.classList.remove('linked');
  els.peerFolderPanel.hidden = true;
  if (clearWatch) watchedEpoch = null;
}

// ---------------------------------------------------------------------------
// Status / bridge UI helpers
// ---------------------------------------------------------------------------
function setStatus(text, meta) {
  els.statusText.textContent = text;
  els.statusMeta.textContent = meta || '';
}

// ---------------------------------------------------------------------------
// File send UI (push model — works identically on desktop & mobile)
// ---------------------------------------------------------------------------
function wireFileUI() {
  els.dropzone.addEventListener('click', (e) => { e.preventDefault(); els.filePicker.click(); });
  els.filePicker.addEventListener('change', () => {
    sendFiles(Array.from(els.filePicker.files));
    els.filePicker.value = '';
  });
  ['dragover', 'dragenter'].forEach((ev) => els.dropzone.addEventListener(ev, (e) => {
    e.preventDefault(); els.dropzone.classList.add('dragover');
  }));
  ['dragleave', 'drop'].forEach((ev) => els.dropzone.addEventListener(ev, (e) => {
    e.preventDefault(); els.dropzone.classList.remove('dragover');
  }));
  els.dropzone.addEventListener('drop', (e) => {
    const files = Array.from(e.dataTransfer.files || []);
    if (files.length) sendFiles(files);
  });
}

function sendFiles(files) {
  if (!transferChannel) { setStatus('Not connected yet — wait for the link.', ''); return; }
  files.forEach((file) => {
    const id = transferChannel.sendFile(file);
    createTransferRow(id, file.name, file.size, 'up');
  });
}

// ---------------------------------------------------------------------------
// Shared folder (host side) — feature-detected, hidden where unsupported
// ---------------------------------------------------------------------------
function wireFolderUI() {
  if (!('showDirectoryPicker' in window)) {
    els.folderPanel.hidden = true;
    return;
  }
  els.folderPanel.hidden = false;
  els.chooseFolderBtn.addEventListener('click', async () => {
    try {
      sharedDirHandle = await window.showDirectoryPicker();
      await refreshSharedFolder();
    } catch (err) {
      if (err.name !== 'AbortError') console.error(err);
    }
  });
  els.refreshPeerFolderBtn.addEventListener('click', () => {
    if (transferChannel) transferChannel.sendControl({ type: 'folder-list-request' });
  });
}

async function refreshSharedFolder() {
  if (!sharedDirHandle) return;
  sharedFileMap.clear();
  const listing = [];
  for await (const [name, handle] of sharedDirHandle.entries()) {
    if (handle.kind !== 'file') continue;
    const file = await handle.getFile();
    sharedFileMap.set(name, handle);
    listing.push({ name, size: file.size });
  }
  els.folderStatus.textContent = `Sharing "${sharedDirHandle.name}" — ${listing.length} file(s) visible to your peer.`;
  announceFolderListing(listing);
}

function announceFolderListing(listing) {
  if (!transferChannel) return;
  if (!listing) {
    listing = Array.from(sharedFileMap.keys()).map((name) => ({ name }));
  }
  transferChannel.sendFolderListing(listing);
}

// ---------------------------------------------------------------------------
// Control-message + transfer-event handlers
// ---------------------------------------------------------------------------
function handleControlMessage(msg) {
  switch (msg.type) {
    case 'folder-list': {
      renderPeerFolder(msg.files || []);
      break;
    }
    case 'folder-list-request': {
      if (sharedDirHandle) refreshSharedFolder();
      else if (transferChannel) transferChannel.sendFolderListing([]);
      break;
    }
    case 'file-request': {
      const handle = sharedFileMap.get(msg.name);
      if (!handle) { transferChannel.sendControl({ type: 'file-error', name: msg.name }); return; }
      handle.getFile().then((file) => {
        const id = transferChannel.sendFile(file);
        createTransferRow(id, file.name, file.size, 'up');
      });
      break;
    }
    case 'file-error': {
      setStatus(`Peer couldn't find "${msg.name}".`, '');
      break;
    }
  }
}

function renderPeerFolder(files) {
  els.peerFolderPanel.hidden = false;
  els.peerFileList.innerHTML = '';
  els.peerFolderEmpty.hidden = files.length > 0;
  files.forEach((f) => {
    const node = peerFileItemTpl.content.cloneNode(true);
    node.querySelector('.peer-file-name').textContent = f.name;
    node.querySelector('.peer-file-size').textContent = f.size != null ? humanBytes(f.size) : '';
    node.querySelector('.peer-file-get').addEventListener('click', (e) => {
      e.target.disabled = true;
      e.target.textContent = 'Requesting…';
      const id = transferChannel.requestFile(f.name);
      createTransferRow(id, f.name, f.size, 'down');
      setTimeout(() => { e.target.disabled = false; e.target.textContent = 'Get'; }, 1500);
    });
    els.peerFileList.appendChild(node);
  });
}

function handleIncomingStart(meta) {
  if (transferRows.has(meta.id)) return; // already created via requestFile()
  createTransferRow(meta.id, meta.name, meta.size, 'down');
}

function handleProgress(id, received, total, dir) {
  const row = transferRows.get(id);
  if (!row) return;
  const pct = total ? Math.min(100, (received / total) * 100) : 0;
  row.fillEl.style.width = pct + '%';
  row.metaEl.firstChild.textContent = `${dir === 'up' ? 'Sending' : 'Receiving'} · ${humanBytes(received)} / ${humanBytes(total)}`;
}

function handleComplete(id, blob, meta) {
  const row = transferRows.get(id);
  if (!row) return;
  row.el.classList.add('done');
  row.fillEl.style.width = '100%';
  if (meta.direction === 'down' && blob) {
    downloadBlob(blob, meta.name);
    row.metaEl.firstChild.textContent = 'Received · saved to Downloads';
  } else {
    row.metaEl.firstChild.textContent = 'Sent';
  }
}

function handleTransferError(id, err) {
  const row = transferRows.get(id);
  if (row) {
    row.el.classList.add('error');
    row.metaEl.firstChild.textContent = 'Failed: ' + (err?.message || 'unknown error');
  }
  console.error('transfer error', id, err);
}

function createTransferRow(id, name, size, dir) {
  els.transferEmpty.hidden = true;
  const node = transferItemTpl.content.cloneNode(true);
  const el = node.querySelector('.transfer-item');
  node.querySelector('.transfer-name').textContent = name;
  node.querySelector('.transfer-size').textContent = size != null ? humanBytes(size) : '';
  const fillEl = node.querySelector('.transfer-bar-fill');
  const metaEl = node.querySelector('.transfer-meta');
  metaEl.appendChild(document.createTextNode(dir === 'up' ? 'Starting…' : 'Waiting for data…'));
  els.transferList.prepend(node);
  const rowEl = els.transferList.firstElementChild;
  transferRows.set(id, { el: rowEl, fillEl: rowEl.querySelector('.transfer-bar-fill'), metaEl: rowEl.querySelector('.transfer-meta') });
}

// ---------------------------------------------------------------------------
// PWA install
// ---------------------------------------------------------------------------
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('sw.js').catch((err) => console.warn('SW registration failed', err));
  });
}
