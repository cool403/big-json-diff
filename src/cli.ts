#!/usr/bin/env node
import { startServer } from './server.js';

const args = process.argv.slice(2);
let port = 8420;
let host = '127.0.0.1';

for (let i = 0; i < args.length; i++) {
  if ((args[i] === '--port' || args[i] === '-p') && args[i + 1]) {
    port = Number(args[++i]);
  } else if ((args[i] === '--host' || args[i] === '-H') && args[i + 1]) {
    host = args[++i];
  } else if (args[i] === '--help' || args[i] === '-h') {
    console.log(`big-json-diff - visual diff for huge JSON files (DuckDB powered)

Usage: big-json-diff [options]

Options:
  -p, --port <port>  Port to listen on (default: 8420)
  -H, --host <host>  Host to bind (default: 127.0.0.1)
  -h, --help         Show this help
`);
    process.exit(0);
  }
}

startServer({ port, host });
