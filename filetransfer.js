/* ==========================================================================
   filetransfer.js
   Wraps a single RTCDataChannel and speaks a small protocol over it:
     - JSON string messages for control (file offers, folder listings, requests)
     - Raw ArrayBuffer messages for file bytes, sent in fixed-size chunks
   Nothing here knows about Firebase or RTCPeerConnection setup — it only
   needs an already-open RTCDataChannel handed to it.
   ========================================================================== */

const CHUNK_SIZE = 16 * 1024;           // 16KB per chunk — safe across browsers
const BUFFERED_AMOUNT_LOW = 1 * 1024 * 1024;  // resume sending below 1MB queued
const BUFFERED_AMOUNT_HIGH = 4 * 1024 * 1024; // pause sending above 4MB queued

function genId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

function humanBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

class TransferChannel {
  /**
   * @param {RTCDataChannel} channel - already open, binaryType 'arraybuffer'
   * @param {object} hooks - { onControl(msg), onProgress(id, receivedBytes, totalBytes, dir),
   *                           onIncomingStart(meta), onComplete(id, blob|null, meta),
   *                           onError(id, err) }
   */
  constructor(channel, hooks = {}) {
    this.channel = channel;
    this.channel.binaryType = 'arraybuffer';
    this.hooks = hooks;

    // outgoing send queue state
    this._sendQueue = [];
    this._sending = false;

    // incoming reassembly state, keyed by transfer id
    this._incoming = new Map();

    this._activeIncomingId = null; // the id currently expecting binary chunks

    this.channel.addEventListener('message', (e) => this._onMessage(e));
    this.channel.bufferedAmountLowThreshold = BUFFERED_AMOUNT_LOW;
  }

  sendControl(obj) {
    if (this.channel.readyState !== 'open') return;
    this.channel.send(JSON.stringify(obj));
  }

  /** Queue a File/Blob for chunked sending. Returns the transfer id. */
  sendFile(file) {
    const id = genId();
    this._sendQueue.push({ id, file });
    this.sendControl({ type: 'file-offer', id, name: file.name, size: file.size, mime: file.type || 'application/octet-stream' });
    this._pumpQueue();
    return id;
  }

  async _pumpQueue() {
    if (this._sending) return;
    const next = this._sendQueue.shift();
    if (!next) return;
    this._sending = true;
    try {
      await this._sendFileChunks(next.id, next.file);
    } catch (err) {
      this.hooks.onError && this.hooks.onError(next.id, err);
    }
    this._sending = false;
    if (this._sendQueue.length) this._pumpQueue();
  }

  async _sendFileChunks(id, file) {
    const size = file.size;
    let offset = 0;
    while (offset < size) {
      if (this.channel.readyState !== 'open') throw new Error('link closed mid-transfer');

      // Backpressure: wait until buffer drains if it's too full
      if (this.channel.bufferedAmount > BUFFERED_AMOUNT_HIGH) {
        await new Promise((resolve) => {
          const check = () => {
            if (this.channel.bufferedAmount <= BUFFERED_AMOUNT_LOW || this.channel.readyState !== 'open') {
              this.channel.removeEventListener('bufferedamountlow', check);
              resolve();
            }
          };
          this.channel.addEventListener('bufferedamountlow', check);
          // Safety poll in case the event doesn't fire on some browsers
          const poll = setInterval(() => {
            if (this.channel.bufferedAmount <= BUFFERED_AMOUNT_LOW || this.channel.readyState !== 'open') {
              clearInterval(poll);
              check();
            }
          }, 60);
        });
      }

      const slice = file.slice(offset, offset + CHUNK_SIZE);
      const buf = await slice.arrayBuffer();

      // Tag every chunk with the transfer id so out-of-order/parallel
      // transfers on the same channel never get cross-contaminated.
      // Format: [4 bytes ascii id-hash][payload] — but to keep this simple
      // and robust we instead prefix each chunk with a small JSON header
      // sent as a separate control message only when the id changes.
      if (this._activeOutId !== id) {
        this.sendControl({ type: 'chunk-stream-begin', id });
        this._activeOutId = id;
      }
      this.channel.send(buf);
      offset += buf.byteLength;

      this.hooks.onProgress && this.hooks.onProgress(id, offset, size, 'up');
    }
    this.sendControl({ type: 'file-done', id });
    this.hooks.onComplete && this.hooks.onComplete(id, null, { name: file.name, size, direction: 'up' });
  }

  sendFolderListing(files) {
    // files: [{name, size}]
    this.sendControl({ type: 'folder-list', files });
  }

  requestFile(name) {
    const id = genId();
    this.sendControl({ type: 'file-request', id, name });
    return id;
  }

  _onMessage(e) {
    if (typeof e.data === 'string') {
      let msg;
      try { msg = JSON.parse(e.data); } catch { return; }
      this._onControlMessage(msg);
    } else {
      this._onBinaryChunk(e.data);
    }
  }

  _onControlMessage(msg) {
    switch (msg.type) {
      case 'file-offer': {
        this._incoming.set(msg.id, {
          id: msg.id,
          name: msg.name,
          size: msg.size,
          mime: msg.mime,
          received: 0,
          chunks: [],
        });
        this.hooks.onIncomingStart && this.hooks.onIncomingStart(msg);
        break;
      }
      case 'chunk-stream-begin': {
        this._activeIncomingId = msg.id;
        break;
      }
      case 'file-done': {
        const rec = this._incoming.get(msg.id);
        if (!rec) return;
        const blob = new Blob(rec.chunks, { type: rec.mime || 'application/octet-stream' });
        this._incoming.delete(msg.id);
        this.hooks.onComplete && this.hooks.onComplete(msg.id, blob, { name: rec.name, size: rec.size, direction: 'down' });
        break;
      }
      default:
        // Everything else (folder-list, file-request, presence pings, etc.)
        // is app-level — hand it up unchanged.
        this.hooks.onControl && this.hooks.onControl(msg);
    }
  }

  _onBinaryChunk(buf) {
    const id = this._activeIncomingId;
    if (!id) return;
    const rec = this._incoming.get(id);
    if (!rec) return;
    rec.chunks.push(buf);
    rec.received += buf.byteLength;
    this.hooks.onProgress && this.hooks.onProgress(id, rec.received, rec.size, 'down');
  }
}

// Helper used by app.js to trigger a browser download from a received Blob.
function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 30000);
}
