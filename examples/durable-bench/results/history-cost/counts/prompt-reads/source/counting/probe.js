(() => {
  // Diagnostic hooks only. No extra Effect operations or schema checks are inserted.
  const labels = new WeakMap();
  const encoder = new TextEncoder();
  let enabled = false;
  let counts = {};
  let scopes = [];
  let cursors = [];
  let records = new Map();
  let modelSnapshots = [];
  let piSeedMax = 0;
  const add = (name, n = 1) => {
    if (!enabled) return;
    counts[name] = (counts[name] ?? 0) + n;
    for (const scope of scopes) {
      const key = scope + "." + name;
      counts[key] = (counts[key] ?? 0) + n;
    }
  };
  globalThis.__hc = {
    add,
    piSeedMax(value) {
      piSeedMax = value;
    },
    enter(name) {
      scopes.push(name);
    },
    leave() {
      scopes.pop();
    },
    label(schema, name) {
      labels.set(schema.ast, name);
      return schema;
    },
    schema(ast, parser) {
      return (input, options) => {
        add("schemaNodeCalls");
        const label = labels.get(ast);
        if (label) add(label);
        return parser(input, options);
      };
    },
    record(stage, envelope) {
      if (!enabled) return;
      add(stage + ".records");
      const record = envelope.record ?? envelope;
      add(stage + ".tag." + record.payload?._tag);
      const id = record.recordId;
      const key = stage + ":" + id;
      records.set(key, (records.get(key) ?? 0) + 1);
    },
    sql(cursor, query) {
      if (enabled)
        cursors.push({ cursor, query: query.replace(/\s+/g, " ").trim(), scopes: [...scopes] });
      return cursor;
    },
    model() {
      if (!enabled) return;
      add("modelCalls");
      modelSnapshots.push({ ...counts });
    },
    start() {
      counts = {};
      scopes = [];
      cursors = [];
      records = new Map();
      modelSnapshots = [];
      enabled = true;
    },
    stop() {
      enabled = false;
      const sql = {};
      for (const { cursor, query, scopes: active } of cursors) {
        const key = active.join("/") + "|" + query;
        const item = (sql[key] ??= { calls: 0, rowsRead: 0, rowsWritten: 0 });
        item.calls++;
        item.rowsRead += cursor.rowsRead;
        item.rowsWritten += cursor.rowsWritten;
      }
      const visits = {};
      for (const [key, n] of records) {
        const stage = key.slice(0, key.indexOf(":"));
        const item = (visits[stage] ??= { unique: 0, histogram: {} });
        item.unique++;
        item.histogram[n] = (item.histogram[n] ?? 0) + 1;
      }
      return { counts, visits, sql, modelSnapshots };
    },
  };
  const parse = JSON.parse;
  JSON.parse = function (text, reviver) {
    const value = parse(text, reviver);
    if (enabled) {
      add("jsonParseCalls");
      add("jsonParseBytes", encoder.encode(text).byteLength);
      if (value?.recordId && value?.payload?._tag) {
        add("canonicalJsonBytes", encoder.encode(text).byteLength);
        globalThis.__hc.record("jsonDecode", value);
      }
      if (value?.model && value?.conversationId) {
        add("piEntryJsonCalls");
        add("piEntryJsonBytes", encoder.encode(text).byteLength);
        add("piEntryMessages", value.model.length);
        const phase = value.id <= piSeedMax ? "piHistoricalEntry" : "piNewEntry";
        add(phase + "Calls");
        add(phase + "Bytes", encoder.encode(text).byteLength);
        const key = "piEntry:" + value.id;
        records.set(key, (records.get(key) ?? 0) + 1);
      }
    }
    return value;
  };
})();
