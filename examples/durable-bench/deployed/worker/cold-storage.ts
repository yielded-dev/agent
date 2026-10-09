import { Schema } from "effect";

import {
  PaddingRequest,
  type PaddingResult,
  type SqlCount,
  type StoragePoint,
} from "./protocol.ts";

const probes = new WeakMap<DurableObjectStorage, StorageProbe>();

export const storageProbe = (storage: DurableObjectStorage) => probes.get(storage);

/** Task-local diagnostic. Product statements and their cursor consumption are unchanged. */
class StorageProbe {
  private statements: Array<{
    sql: string;
    cursor: SqlStorageCursor<Record<string, SqlStorageValue>>;
  }> = [];
  private points: Array<typeof StoragePoint.Type> = [];
  private started = false;
  touchedPaddingBytes = 0;
  private readonly nativeExec;
  constructor(readonly raw: DurableObjectStorage) {
    this.nativeExec = raw.sql.exec.bind(raw.sql);
  }
  exec(sql: string, ...bindings: SqlStorageValue[]) {
    const cursor = this.nativeExec(sql, ...bindings);

    this.statements.push({ sql, cursor });

    return cursor;
  }
  initialize() {
    const exists = this.exec(
      "SELECT name FROM sqlite_master WHERE name = 'cold_storage_probe_config'",
    ).toArray();

    if (!exists.length) return;
    const config = this.exec("SELECT read_padding FROM cold_storage_probe_config").one();

    if (config.read_padding === 1) {
      const result = this.exec(
        "SELECT COALESCE(SUM(length(payload)), 0) AS bytes FROM cold_storage_probe_padding",
      ).one();

      this.touchedPaddingBytes = Schema.decodeUnknownSync(Schema.Natural)(result.bytes);
    }
  }
  begin() {
    if (this.started) this.statements = [];
    this.started = true;
    this.points = [];
    this.point("entry");
  }
  point(phase: string) {
    const grouped = new Map<string, typeof SqlCount.Type>();

    for (const { sql, cursor } of this.statements) {
      const key = sql.replace(/\s+/g, " ").trim();
      const prior = grouped.get(key);

      grouped.set(key, {
        sql: key,
        count: (prior?.count ?? 0) + 1,
        read: (prior?.read ?? 0) + cursor.rowsRead,
        written: (prior?.written ?? 0) + cursor.rowsWritten,
      });
    }
    this.points.push({ phase, atMs: Date.now(), sql: [...grouped.values()] });
  }
  report() {
    this.point("settled");

    return { points: this.points, touchedPaddingBytes: this.touchedPaddingBytes };
  }
  padding(input: unknown): typeof PaddingResult.Type {
    const { mib, read } = Schema.decodeUnknownSync(PaddingRequest)(input);
    const sql = this.raw.sql;

    this.raw.transactionSync(() => {
      sql.exec(
        "CREATE TABLE IF NOT EXISTS cold_storage_probe_config (read_padding INTEGER NOT NULL)",
      );
      sql.exec(
        "CREATE TABLE IF NOT EXISTS cold_storage_probe_padding (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
      );
      sql.exec("DELETE FROM cold_storage_probe_config");
      sql.exec("INSERT INTO cold_storage_probe_config VALUES (?)", Number(read));
      sql.exec("DELETE FROM cold_storage_probe_padding");
      let state = 0xc01d;
      const bytes = new Uint8Array(4096);
      const decoder = new TextDecoder();

      for (let row = 0; row < mib * 256; row++) {
        for (let byte = 0; byte < bytes.length; byte++) {
          state ^= state << 13;
          state ^= state >>> 17;
          state ^= state << 5;
          bytes[byte] = 32 + ((state >>> 24) & 63);
        }
        sql.exec(
          "INSERT INTO cold_storage_probe_padding VALUES (?, ?)",
          row,
          decoder.decode(bytes),
        );
      }
    });
    const unavailable: string[] = [];

    const pragma = (name: string): number | null => {
      try {
        const row = sql.exec(`PRAGMA ${name}`).one();

        return Schema.decodeUnknownSync(Schema.Number)(row[name]);
      } catch {
        unavailable.push(name);

        return null;
      }
    };

    return {
      ok: true,
      bytes: sql.databaseSize,
      paddingBytes: mib * 1024 * 1024,
      read,
      pageSize: pragma("page_size"),
      pageCount: pragma("page_count"),
      freelistCount: pragma("freelist_count"),
      unavailable,
    };
  }
}

export const instrumentStorage = (ctx: DurableObjectState): DurableObjectState => {
  const meter = new StorageProbe(ctx.storage);

  Object.defineProperty(ctx.storage.sql, "exec", { value: meter.exec.bind(meter) });
  probes.set(ctx.storage, meter);
  meter.initialize();

  return ctx;
};
