import { createServer, IncomingMessage, ServerResponse } from 'node:http';
import { createReadStream, createWriteStream, existsSync } from 'node:fs';
import { mkdir, rm } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';
import { DiffEngine } from './diff.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.resolve(__dirname, '..', 'public');

interface Upload {
  id: string;
  name: string;
  size: number;
  file: string;
}

export interface ServerOptions {
  port: number;
  host: string;
}

function send(res: ServerResponse, status: number, body: string | object, type = 'application/json') {
  const data = typeof body === 'string' ? body : JSON.stringify(body);
  res.writeHead(status, { 'content-type': type });
  res.end(data);
}

export async function startServer(opts: ServerOptions) {
  const workDir = path.join(tmpdir(), `big-json-diff-${process.pid}`);
  await mkdir(workDir, { recursive: true });

  const uploads = new Map<string, Upload>();

  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);

    if (req.method === 'GET' && url.pathname === '/') {
      const html = path.join(PUBLIC_DIR, 'index.html');
      if (!existsSync(html)) return send(res, 500, 'index.html missing', 'text/plain');
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      createReadStream(html).pipe(res);
      return;
    }

    if (req.method === 'GET' && url.pathname === '/healthz') {
      return send(res, 200, { ok: true });
    }

    // Raw body upload: POST /upload?name=foo.json  -> { id, name, size }
    if (req.method === 'POST' && url.pathname === '/upload') {
      const id = randomUUID();
      const name = (url.searchParams.get('name') || 'file.json').slice(0, 256);
      const file = path.join(workDir, `${id}.json`);
      const ws = createWriteStream(file);
      let size = 0;
      req.on('data', (c: Buffer) => (size += c.length));
      req.pipe(ws);
      ws.on('finish', () => {
        uploads.set(id, { id, name, size, file });
        send(res, 200, { id, name, size });
      });
      ws.on('error', (e) => send(res, 500, { error: String(e) }));
      req.on('error', (e) => send(res, 500, { error: String(e) }));
      return;
    }

    send(res, 404, { error: 'not found' });
  });

  const wss = new WebSocketServer({ server, path: '/ws' });

  wss.on('connection', (ws: WebSocket) => {
    const sendMsg = (msg: object) => {
      if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
    };
    const waitDrain = async () => {
      while (ws.bufferedAmount > 8 * 1024 * 1024) {
        await new Promise((r) => setTimeout(r, 20));
        if (ws.readyState !== ws.OPEN) return;
      }
    };

    ws.on('message', async (data) => {
      let msg: { type?: string; a?: string; b?: string };
      try {
        msg = JSON.parse(String(data));
      } catch {
        return;
      }
      if (msg.type !== 'compare') return;

      const upA = msg.a ? uploads.get(msg.a) : undefined;
      const upB = msg.b ? uploads.get(msg.b) : undefined;
      if (!upA || !upB) {
        sendMsg({ type: 'error', message: 'upload not found, please re-upload both files' });
        return;
      }

      const engine = new DiffEngine(workDir);
      try {
        sendMsg({ type: 'status', stage: 'load', detail: `DuckDB 正在加载 ${upA.name} 与 ${upB.name}…` });
        const [na, nb] = await engine.loadBoth(upA.file, upB.file);

        sendMsg({ type: 'status', stage: 'diff', detail: '正在计算差异…' });
        const summary = await engine.buildDiff();
        sendMsg({ type: 'summary', summary, stats: { aLeaves: na, bLeaves: nb } });

        let sent = 0;
        await engine.streamDiff(async (rows) => {
          sent += rows.length;
          sendMsg({ type: 'batch', rows });
          await waitDrain();
        });
        sendMsg({ type: 'done' });
      } catch (e) {
        sendMsg({ type: 'error', message: e instanceof Error ? e.message : String(e) });
      } finally {
        engine.close();
      }
    });
  });

  await new Promise<void>((resolve) => server.listen(opts.port, opts.host, resolve));

  const url = `http://${opts.host}:${opts.port}`;
  console.log(`\n  big-json-diff running at  ${url}\n  Press Ctrl+C to stop.\n`);

  const cleanup = async () => {
    wss.close();
    server.close();
    await rm(workDir, { recursive: true, force: true });
    process.exit(0);
  };
  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);
}
