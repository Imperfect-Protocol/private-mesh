import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const rootDir = join(__dirname, '..');
const publicDir = join(rootDir, 'public');

const httpHost = process.env.PRIVATE_MESH_HTTP_HOST ?? '0.0.0.0';
const httpPort = Number(process.env.PRIVATE_MESH_HTTP_PORT ?? 8787);
const maxBodyBytes = Number(process.env.PRIVATE_MESH_MAX_BODY_BYTES ?? 1024 * 1024);
const maxRetainedPerRecipient = Number(process.env.PRIVATE_MESH_MAX_RETAINED ?? 200);
const signalTtlMs = Number(process.env.PRIVATE_MESH_SIGNAL_TTL_MS ?? 120_000);

const contentTypes = new Map([
  ['.html', 'text/html; charset=utf-8'],
  ['.css', 'text/css; charset=utf-8'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.svg', 'image/svg+xml'],
]);

const retained = new Map();
const streams = new Map();

function now() {
  return Date.now();
}

function normalizeHex(value) {
  const text = String(value ?? '').trim();
  return text.startsWith('0x') ? `0x${text.slice(2).toLowerCase()}` : `0x${text.toLowerCase()}`;
}

function normalizeAddress(value) {
  const hex = normalizeHex(value);
  if (!/^0x[0-9a-f]{40}$/.test(hex)) return '';
  return hex;
}

function parseSignalFrame(raw) {
  const hex = normalizeHex(raw.trim());
  if (!/^0x[0-9a-f]+$/.test(hex)) {
    throw Object.assign(new Error('signal must be hex'), { statusCode: 400 });
  }
  if (hex.length < 2 + 40 + 2) {
    throw Object.assign(new Error('signal must contain recipient address and payload'), { statusCode: 400 });
  }
  if ((hex.length - 2) % 2 !== 0) {
    throw Object.assign(new Error('signal hex must contain full bytes'), { statusCode: 400 });
  }

  const recipient = `0x${hex.slice(2, 42)}`;
  const payload = `0x${hex.slice(42)}`;
  return { recipient, payload, signal: hex };
}

function parseSignalFrames(raw) {
  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (lines.length === 0) {
    throw Object.assign(new Error('signal body required'), { statusCode: 400 });
  }
  return lines.map(parseSignalFrame);
}

function cleanupRecipient(recipient) {
  const key = normalizeAddress(recipient);
  const messages = retained.get(key);
  if (!messages) return;
  const cutoff = now() - signalTtlMs;
  const fresh = messages.filter((message) => message.sent_at >= cutoff).slice(-maxRetainedPerRecipient);
  if (fresh.length > 0) {
    retained.set(key, fresh);
  } else {
    retained.delete(key);
  }
}

function retain(signal) {
  cleanupRecipient(signal.recipient);
  const messages = retained.get(signal.recipient) ?? [];
  messages.push(signal);
  retained.set(signal.recipient, messages.slice(-maxRetainedPerRecipient));
}

function publish(signal) {
  const subscribers = streams.get(signal.recipient);
  if (!subscribers) return;
  for (const res of subscribers) {
    res.write(`id: ${signal.id}\n`);
    res.write('event: signal\n');
    res.write(`data: ${JSON.stringify(signal)}\n\n`);
  }
}

function sendJson(res, status, value) {
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,PUT,DELETE,OPTIONS',
    'access-control-allow-headers': 'content-type',
  });
  res.end(status === 204 ? '' : JSON.stringify(value, null, 2));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => {
      body += chunk;
      if (body.length > maxBodyBytes) {
        reject(Object.assign(new Error('signal too large'), { statusCode: 413 }));
        req.destroy();
      }
    });
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

async function handlePutSignal(req, res) {
  const raw = await readBody(req);
  const parsedSignals = parseSignalFrames(raw);
  const signals = parsedSignals.map((parsed) => ({
    id: randomUUID(),
    sent_at: now(),
    expires_at: now() + signalTtlMs,
    recipient: parsed.recipient,
    payload: parsed.payload,
    signal: parsed.signal,
  }));

  for (const signal of signals) {
    retain(signal);
    publish(signal);
  }

  console.log('[mesh] signals', {
    count: signals.length,
    bytes: raw.length,
  });

  sendJson(res, 202, {
    accepted: true,
    ids: signals.map((signal) => signal.id),
  });
}

function listSignals(query) {
  const recipient = normalizeAddress(query.get('recipient') ?? query.get('to'));
  if (!recipient) return [];
  cleanupRecipient(recipient);
  const after = query.get('after') ?? '';
  return (retained.get(recipient) ?? []).filter((signal) => !after || signal.id > after);
}

function handleSignalStream(req, res, query) {
  const recipient = normalizeAddress(query.get('recipient') ?? query.get('to'));
  if (!recipient) {
    sendJson(res, 400, { error: 'recipient required' });
    return;
  }

  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-store',
    connection: 'keep-alive',
    'access-control-allow-origin': '*',
  });
  res.write(': connected\n\n');

  const subscribers = streams.get(recipient) ?? new Set();
  subscribers.add(res);
  streams.set(recipient, subscribers);

  for (const signal of listSignals(query)) {
    res.write(`id: ${signal.id}\n`);
    res.write('event: signal\n');
    res.write(`data: ${JSON.stringify(signal)}\n\n`);
  }

  req.on('close', () => {
    subscribers.delete(res);
    if (subscribers.size === 0) streams.delete(recipient);
  });
}

function safePublicPath(urlPath) {
  const pathname = decodeURIComponent(new URL(urlPath, 'http://private.mesh').pathname);
  const requested = pathname === '/' ? '/index.html' : pathname;
  const normalized = normalize(requested).replace(/^(\.\.(\/|\\|$))+/, '');
  return join(publicDir, normalized);
}

async function serveStatic(req, res) {
  const filePath = safePublicPath(req.url ?? '/');
  if (!filePath.startsWith(publicDir)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  const body = await readFile(filePath);
  res.writeHead(200, {
    'content-type': contentTypes.get(extname(filePath)) ?? 'application/octet-stream',
    'cache-control': 'no-store',
  });
  res.end(body);
}

const server = createServer(async (req, res) => {
  try {
    if (req.method === 'OPTIONS') {
      sendJson(res, 204, {});
      return;
    }

    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'private.mesh'}`);

    if (req.method === 'GET' && url.pathname === '/mesh.json') {
      sendJson(res, 200, {
        name: 'Private Mesh',
        version: 1,
        endpoints: {
          send: 'PUT /signal',
          poll: 'GET /signal?recipient=<address>',
          stream: 'GET /signal/stream?recipient=<address>',
        },
        retainedRecipients: retained.size,
        openStreams: [...streams.values()].reduce((sum, subscribers) => sum + subscribers.size, 0),
        ttlMs: signalTtlMs,
      });
      return;
    }

    if (req.method === 'PUT' && url.pathname === '/signal') {
      await handlePutSignal(req, res);
      return;
    }

    if (req.method === 'GET' && url.pathname === '/signal') {
      sendJson(res, 200, { signals: listSignals(url.searchParams) });
      return;
    }

    if (req.method === 'GET' && url.pathname === '/signal/stream') {
      handleSignalStream(req, res, url.searchParams);
      return;
    }

    if (req.method === 'DELETE' && url.pathname === '/signal') {
      const recipient = normalizeAddress(url.searchParams.get('recipient') ?? url.searchParams.get('to'));
      if (recipient) retained.delete(recipient);
      sendJson(res, 200, { cleared: Boolean(recipient) });
      return;
    }

    if (req.method === 'GET' || req.method === 'HEAD') {
      await serveStatic(req, res);
      return;
    }

    sendJson(res, 405, { error: 'method not allowed' });
  } catch (error) {
    if (error?.code === 'ENOENT') {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    console.error('[mesh] error', error);
    sendJson(res, error?.statusCode ?? 500, { error: error?.message ?? 'internal server error' });
  }
});

const cleanupTimer = setInterval(() => {
  for (const recipient of retained.keys()) cleanupRecipient(recipient);
}, Math.min(signalTtlMs, 30_000));

server.listen(httpPort, httpHost, () => {
  console.log('[mesh] Private Mesh');
  console.log(`[mesh] ui http://${httpHost === '0.0.0.0' ? '127.0.0.1' : httpHost}:${httpPort}`);
  console.log('[mesh] send   PUT /signal');
  console.log('[mesh] stream GET /signal/stream?recipient=<address>');
});

function shutdown() {
  console.log('[mesh] shutdown');
  clearInterval(cleanupTimer);
  server.close(() => process.exit(0));
}

process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
