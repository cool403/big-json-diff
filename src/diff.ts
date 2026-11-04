import { writeFileSync, rmSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import duckdb from 'duckdb';

export interface DiffRow {
  path: string;
  kind: 'added' | 'removed' | 'changed';
  a: string | null;
  b: string | null;
}

export interface DiffSummary {
  added: number;
  removed: number;
  changed: number;
}

const MAX_VALUE_LEN = 4000;
/** Containers at or below this size are expanded whole via json_tree (fast). */
const SMALL_CONTAINER_BYTES = 1_000_000;

function sqlString(s: string): string {
  return `'${s.replace(/'/g, "''")}'`;
}

function sqlIdent(s: string): string {
  return `"${s.replace(/"/g, '""')}"`;
}

function truncate(v: string | null): string | null {
  if (v == null) return v;
  if (v.length > MAX_VALUE_LEN) return v.slice(0, MAX_VALUE_LEN) + '…';
  return v;
}

export class DiffEngine {
  private db: duckdb.Database;
  private conn: duckdb.Connection;
  private tmpDir: string;

  constructor(tmpDir: string) {
    this.db = new duckdb.Database(':memory:');
    this.conn = this.db.connect();
    this.tmpDir = tmpDir;
  }

  private runOn(conn: duckdb.Connection, sql: string): Promise<void> {
    if (process.env.BJD_DEBUG) console.error('[sql]', sql.replace(/\s+/g, ' ').slice(0, 120));
    return new Promise((resolve, reject) => {
      conn.run(sql, (err) => (err ? reject(err) : resolve()));
    });
  }

  private allOn<T>(conn: duckdb.Connection, sql: string): Promise<T[]> {
    return new Promise((resolve, reject) => {
      conn.all(sql, (err, rows) => (err ? reject(err) : resolve(rows as T[])));
    });
  }

  private run(sql: string): Promise<void> {
    return this.runOn(this.conn, sql);
  }

  private all<T = Record<string, unknown>>(sql: string): Promise<T[]> {
    return this.allOn<T>(this.conn, sql);
  }

  /**
   * Flatten a JSON file into leaf rows (path, val).
   *
   * Strategy: small containers (<=1MB) are expanded in one shot with a
   * lateral json_tree (very fast on row-sized documents). Big containers
   * hit a pathological slow path in DuckDB's in-row JSON functions, so they
   * are spilled to a temp file and expanded ONE level by DuckDB's streaming
   * file readers (read_json / read_json_auto), which are fast and
   * memory-safe. The loop then re-examines the (smaller) children.
   *
   * Each side runs in its own in-memory Database (own memory budget, true
   * parallelism) and hands the leaves back to the main DB via parquet.
   */
  async loadSide(table: 'flat_a' | 'flat_b', filePath: string): Promise<number> {
    const sideDb = new duckdb.Database(':memory:');
    const conn = sideDb.connect();
    const flat = 'work';
    const out = 'leaves';
    try {
      await this.runOn(conn, `CREATE TABLE ${flat} AS
        SELECT '$' AS path, json(content) AS j FROM read_text(${sqlString(filePath)})`);
      await this.runOn(conn, `CREATE TABLE ${out}(path VARCHAR, val VARCHAR)`);

      for (let iter = 0; iter < 100; iter++) {
        // scalar rows -> out
        await this.runOn(conn, `INSERT INTO ${out}
          SELECT path, coalesce(j::VARCHAR, 'null') FROM ${flat}
          WHERE j IS NULL OR json_type(j) NOT IN ('OBJECT', 'ARRAY')`);
        await this.runOn(conn, `DELETE FROM ${flat}
          WHERE j IS NULL OR json_type(j) NOT IN ('OBJECT', 'ARRAY')`);
        const remaining = await this.allOn<{ n: number }>(conn, `SELECT count(*) AS n FROM ${flat}`);
        if (Number(remaining[0].n) === 0) break;

        // Split by size FIRST: a WHERE clause does not protect the lateral
        // json_tree from being evaluated on huge rows (predicate order is
        // not guaranteed), so we physically partition the working table.
        await this.runOn(conn, `CREATE OR REPLACE TABLE small AS
          SELECT * FROM ${flat} WHERE length(j::VARCHAR) <= ${SMALL_CONTAINER_BYTES}`);
        await this.runOn(conn, `CREATE OR REPLACE TABLE big AS
          SELECT * FROM ${flat} WHERE length(j::VARCHAR) > ${SMALL_CONTAINER_BYTES}`);
        await this.runOn(conn, `DELETE FROM ${flat}`);

        // small containers -> expand whole subtree in one pass
        await this.runOn(conn, `INSERT INTO ${out}
          SELECT f.path || substring(jt.fullkey, 2), jt.value::VARCHAR
          FROM small f, json_tree(f.j) jt
          WHERE jt.type NOT IN ('OBJECT', 'ARRAY') OR jt.value::VARCHAR IN ('{}', '[]')`);
        await this.runOn(conn, `DROP TABLE small`);

        // big containers -> spill to temp file, expand one level via file reader
        const big = await this.allOn<{ path: string; j: string }>(
          conn, `SELECT path, j::VARCHAR AS j FROM big`
        );
        await this.runOn(conn, `DROP TABLE big`);
        for (const row of big) {
          await this.expandBig(conn, flat, row.path, row.j);
        }
      }

      const tag = table === 'flat_a' ? 'a' : 'b';
      const pq = path.join(this.tmpDir, `bjd-leaves-${tag}.parquet`);
      rmSync(pq, { force: true });
      await this.runOn(conn, `COPY (SELECT path, val FROM ${out}) TO ${sqlString(pq)} (FORMAT PARQUET)`);
      await this.run(`CREATE OR REPLACE TABLE ${table} AS
        SELECT path, val FROM read_parquet(${sqlString(pq)})`);
      rmSync(pq, { force: true });
    } finally {
      conn.close();
      sideDb.close();
    }
    const rows = await this.all<{ n: number }>(`SELECT count(*) AS n FROM ${table}`);
    return Number(rows[0].n);
  }

  /** Expand one big container one level; children are inserted into `flat`. */
  private async expandBig(
    conn: duckdb.Connection, flat: string, parentPath: string, j: string
  ): Promise<void> {
    const tmp = path.join(this.tmpDir, `bjd-${randomUUID()}.json`);
    writeFileSync(tmp, j);
    const maxObj = Math.max(j.length * 2, 16 * 1024 * 1024);
    try {
      if (j.startsWith('[')) {
        // Big array: stream elements back as faithful JSON strings.
        await this.runOn(conn, `INSERT INTO ${flat}
          SELECT ${sqlString(parentPath)} || '[' || (row_number() OVER () - 1)::VARCHAR || ']',
                 coalesce(k.j, json('null'))
          FROM read_json(${sqlString(tmp)}, format='array', columns={json: 'JSON'},
                         maximum_object_size=${maxObj}) AS k(j)`);
      } else {
        // Big object: typed row via read_json_auto; scalars/structs are
        // serialized with to_json, LIST columns are unnested natively.
        const t = `big_${randomUUID().replace(/-/g, '')}`;
        await this.runOn(conn, `CREATE TEMP TABLE ${t} AS
          SELECT * FROM read_json_auto(${sqlString(tmp)}, format='auto',
                                       maximum_object_size=${maxObj})`);
        const cols = await this.allOn<{ column_name: string; column_type: string }>(
          conn, `DESCRIBE ${t}`
        );
        const parts: string[] = [];
        for (const col of cols) {
          const id = sqlIdent(col.column_name);
          const childPath = `${parentPath}.${col.column_name}`;
          if (col.column_type.endsWith('[]')) {
            parts.push(`SELECT ${sqlString(childPath)} || '[' || (u.i - 1)::VARCHAR || ']',
              coalesce(to_json(u.v), json('null'))
              FROM ${t}, unnest(${t}.${id}) WITH ORDINALITY AS u(v, i)`);
            // keep empty lists visible
            parts.push(`SELECT ${sqlString(childPath)}, json('[]')
              FROM ${t} WHERE length(${t}.${id}) = 0`);
          } else {
            parts.push(`SELECT ${sqlString(childPath)},
              coalesce(to_json(${t}.${id}), json('null')) FROM ${t}`);
          }
        }
        if (parts.length > 0) {
          await this.runOn(conn, `INSERT INTO ${flat} ${parts.join(' UNION ALL ')}`);
        }
        await this.runOn(conn, `DROP TABLE ${t}`);
      }
    } finally {
      rmSync(tmp, { force: true });
    }
  }

  /** Load both sides concurrently (each in its own Database). */
  loadBoth(fileA: string, fileB: string): Promise<[number, number]> {
    return Promise.all([
      this.loadSide('flat_a', fileA),
      this.loadSide('flat_b', fileB),
    ]);
  }

  /** Build the diff table and return the summary. */
  async buildDiff(): Promise<DiffSummary> {
    await this.run(`CREATE OR REPLACE TABLE diff AS
      SELECT coalesce(a.path, b.path) AS path,
             CASE WHEN a.path IS NULL THEN 'added'
                  WHEN b.path IS NULL THEN 'removed'
                  ELSE 'changed' END AS kind,
             a.val AS a, b.val AS b
      FROM flat_a a FULL OUTER JOIN flat_b b ON a.path = b.path
      WHERE a.path IS NULL OR b.path IS NULL OR a.val IS DISTINCT FROM b.val`);
    const rows = await this.all<{ kind: string; n: number }>(
      `SELECT kind, count(*) AS n FROM diff GROUP BY kind`
    );
    const summary: DiffSummary = { added: 0, removed: 0, changed: 0 };
    for (const r of rows) summary[r.kind as keyof DiffSummary] = Number(r.n);
    return summary;
  }

  /** Stream diff rows ordered by path in batches. */
  async streamDiff(onBatch: (rows: DiffRow[]) => Promise<void>): Promise<void> {
    const CHUNK = 5000;
    let offset = 0;
    for (;;) {
      const rows = await this.all<{ path: string; kind: string; a: string | null; b: string | null }>(
        `SELECT path, kind, a, b FROM diff ORDER BY path LIMIT ${CHUNK} OFFSET ${offset}`
      );
      if (rows.length === 0) break;
      await onBatch(
        rows.map((r) => ({
          path: r.path,
          kind: r.kind as DiffRow['kind'],
          a: truncate(r.a),
          b: truncate(r.b),
        }))
      );
      offset += rows.length;
      if (rows.length < CHUNK) break;
    }
  }

  close(): void {
    this.conn.close();
    this.db.close();
  }
}
