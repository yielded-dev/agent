-- Frozen layout-16 store from source commit 12200fc216397c5182e52343f0f0ff7650de8c33.
-- Existing run-checkpoint worker exited 137 at save-recovery-checkpoint:after.
-- Retain the original log, admissions, ownership and recovery cache independently of current writers.
PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;
CREATE TABLE "effect_agent_abort_intents" (
      submission_id TEXT PRIMARY KEY NOT NULL,
      author TEXT NOT NULL,
      reason TEXT NOT NULL,
      requested_at TEXT NOT NULL,
      canonical_record_id TEXT,
      FOREIGN KEY (submission_id)
        REFERENCES "effect_agent_submissions"(submission_id)
        ON DELETE RESTRICT
    );
CREATE TABLE "effect_agent_approval_decisions" (
      submission_id TEXT NOT NULL,
      tool_call_id TEXT NOT NULL,
      decision TEXT NOT NULL,
      resolver TEXT NOT NULL,
      reason TEXT NOT NULL,
      decided_at TEXT NOT NULL,
      PRIMARY KEY (submission_id, tool_call_id),
      FOREIGN KEY (submission_id)
        REFERENCES "effect_agent_submissions"(submission_id)
        ON DELETE RESTRICT
    );
CREATE TABLE "effect_agent_attempts" (
      attempt_id TEXT PRIMARY KEY NOT NULL,
      submission_id TEXT NOT NULL,
      thread_id TEXT NOT NULL,
      owner_producer_id TEXT NOT NULL,
      producer_epoch INTEGER NOT NULL,
      claimed_at TEXT NOT NULL,
      FOREIGN KEY (submission_id)
        REFERENCES "effect_agent_submissions"(submission_id)
        ON DELETE RESTRICT
    );
INSERT INTO "effect_agent_attempts" VALUES('attempt-01a1099c-1e2e-7693-983c-897ca64334d0','submission-01a1099c-1e29-7058-ae6b-921be7af7937','checkpoint-crash','producer-crash-child',1,'2026-10-05T01:09:48.718Z');
CREATE TABLE "effect_agent_canonical_batches" (
      thread_id TEXT NOT NULL,
      batch_id TEXT NOT NULL,
      first_sequence INTEGER NOT NULL,
      last_sequence INTEGER NOT NULL,
      batch_digest TEXT NOT NULL,
      tail_digest TEXT NOT NULL,
      batch_json TEXT NOT NULL,
      PRIMARY KEY (thread_id, batch_id),
      FOREIGN KEY (thread_id)
        REFERENCES "effect_agent_threads"(thread_id)
        ON DELETE RESTRICT
    );
INSERT INTO "effect_agent_canonical_batches" VALUES('checkpoint-crash','thread-created:checkpoint-crash',1,1,'559d05e0b00ffd92dbd5cedd47b9dc830e57f1a9ba55201e75c1f7775d2e8c24','559d05e0b00ffd92dbd5cedd47b9dc830e57f1a9ba55201e75c1f7775d2e8c24','{"batchId":"thread-created:checkpoint-crash","producerId":"producer-crash-child","records":[{"createdAt":"2026-10-05T01:09:48.716Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"ThreadCreated","agentId":"crash-checkpoint","definitions":{"agent":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","model":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tools":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},"recordId":"thread-created:checkpoint-crash","schemaVersion":1}]}');
INSERT INTO "effect_agent_canonical_batches" VALUES('checkpoint-crash','submission-input:submission-01a1099c-1e29-7058-ae6b-921be7af7937',2,2,'d938e8ae13427d02e458f13eca7b70f7373650037b5358c8a6c669a6f7fc4bba','d938e8ae13427d02e458f13eca7b70f7373650037b5358c8a6c669a6f7fc4bba','{"batchId":"submission-input:submission-01a1099c-1e29-7058-ae6b-921be7af7937","producerId":"producer-crash-child","records":[{"createdAt":"2026-10-05T01:09:48.725Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"UserInputRecorded","input":{"question":"does accepted work survive a process kill?"},"kind":"user","runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","submissionId":"submission-01a1099c-1e29-7058-ae6b-921be7af7937"},"recordId":"input:submission-01a1099c-1e29-7058-ae6b-921be7af7937","schemaVersion":1}]}');
INSERT INTO "effect_agent_canonical_batches" VALUES('checkpoint-crash','run-start:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937',3,3,'7bd14ef1752394bd0b73dc044fb30c61953a0d7035b4ab5d1a021f7754298e59','7bd14ef1752394bd0b73dc044fb30c61953a0d7035b4ab5d1a021f7754298e59','{"batchId":"run-start:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","producerId":"producer-crash-child","records":[{"createdAt":"2026-10-05T01:09:48.727Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"RunStarted","maxDurationMillis":30000,"policyAccountingVersion":1,"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937"},"recordId":"run-start:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","schemaVersion":1}]}');
INSERT INTO "effect_agent_canonical_batches" VALUES('checkpoint-crash','turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1',4,5,'83c69497459959c3bf4af5e306b6d78622fc1df6a7c3db76083d51d43bab1ad7','83c69497459959c3bf4af5e306b6d78622fc1df6a7c3db76083d51d43bab1ad7','{"batchId":"turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1","producerId":"producer-crash-child","records":[{"createdAt":"2026-10-05T01:09:48.748Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"ModelResponseRecorded","inputTokens":100,"messages":{"content":[{"content":"Keep original checkpoint instructions.","options":{},"role":"system"},{"content":"{\"question\":\"does accepted work survive a process kill?\"}","options":{},"role":"user"},{"content":[{"id":"checkpoint-call-1","name":"search","options":{},"params":{"query":"checkpoint-read-1"},"providerExecuted":false,"type":"tool-call"}],"options":{},"role":"assistant"}]},"messagesDigest":"fa7f2343f0193a45c028ced635ec14d10cd819c0d35db98aa13b1ea3f85776ee","modelUsage":[{"costMicrousd":0,"inputTokens":{"cacheRead":0,"cacheWrite":0,"total":100,"uncached":100},"model":"crash-harness","outputTokens":{"reasoning":0,"text":10,"total":10},"pricingStatus":"unknown","provider":"scripted","purpose":"turn","usageStatus":"partial","webSearchCalls":0}],"outputTokens":10,"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","runScopedPrefixLength":2,"toolOperations":[{"executionClass":"readonly","executionKind":"ordinary","replay":"cdc2a046e08d84beb8ae1b6d4b3c3578d6f9f5c5fae56d4fd05665cf77ffd412","toolCallId":"checkpoint-call-1","toolName":"search"}],"turn":1,"turnId":"turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1"},"recordId":"model-response:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1","schemaVersion":1},{"createdAt":"2026-10-05T01:09:48.754Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"ToolCallSettled","isFailure":false,"result":{"available":true},"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","toolCallId":"checkpoint-call-1","toolName":"search"},"recordId":"tool-settled:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1:checkpoint-call-1","schemaVersion":1}]}');
INSERT INTO "effect_agent_canonical_batches" VALUES('checkpoint-crash','turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2',6,7,'3f7617ee4ee3ab426225d5fa69fbee2ce69398c18dae6d279a1fe2105e82ca38','3f7617ee4ee3ab426225d5fa69fbee2ce69398c18dae6d279a1fe2105e82ca38','{"batchId":"turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2","producerId":"producer-crash-child","records":[{"createdAt":"2026-10-05T01:09:48.757Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"ModelResponseRecorded","inputTokens":100,"messages":{"content":[{"content":[{"id":"checkpoint-call-2","name":"search","options":{},"params":{"query":"checkpoint-read-2"},"providerExecuted":false,"type":"tool-call"}],"options":{},"role":"assistant"}]},"messagesDigest":"34623417e140cf96e6e4b1956c067d3d7524d92c130d191fc16288b3129aef84","modelUsage":[{"costMicrousd":0,"inputTokens":{"cacheRead":0,"cacheWrite":0,"total":100,"uncached":100},"model":"crash-harness","outputTokens":{"reasoning":0,"text":10,"total":10},"pricingStatus":"unknown","provider":"scripted","purpose":"turn","usageStatus":"partial","webSearchCalls":0}],"outputTokens":10,"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","toolOperations":[{"executionClass":"readonly","executionKind":"ordinary","replay":"cdc2a046e08d84beb8ae1b6d4b3c3578d6f9f5c5fae56d4fd05665cf77ffd412","toolCallId":"checkpoint-call-2","toolName":"search"}],"turn":2,"turnId":"turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2"},"recordId":"model-response:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2","schemaVersion":1},{"createdAt":"2026-10-05T01:09:48.758Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"ToolCallSettled","isFailure":false,"result":{"available":true},"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","toolCallId":"checkpoint-call-2","toolName":"search"},"recordId":"tool-settled:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2:checkpoint-call-2","schemaVersion":1}]}');
INSERT INTO "effect_agent_canonical_batches" VALUES('checkpoint-crash','compaction:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:3:rollover',8,8,'4441eb6584c9530b7b4788585c3845635625ec098f90c687c3e2c5888c18af57','4441eb6584c9530b7b4788585c3845635625ec098f90c687c3e2c5888c18af57','{"batchId":"compaction:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:3:rollover","producerId":"producer-crash-child","records":[{"createdAt":"2026-10-05T01:09:48.763Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"CompactionCreated","coversThrough":7,"handoff":"Keep the checkpoint continuation.","kind":"rollover","runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","turn":3},"recordId":"compaction:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:3:rollover","schemaVersion":1}]}');
CREATE TABLE "effect_agent_canonical_records" (
      thread_id TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      record_id TEXT NOT NULL,
      batch_id TEXT NOT NULL,
      record_json TEXT NOT NULL,
      PRIMARY KEY (thread_id, sequence),
      UNIQUE (thread_id, record_id),
      FOREIGN KEY (thread_id, batch_id)
        REFERENCES "effect_agent_canonical_batches"(thread_id, batch_id)
        ON DELETE RESTRICT
    );
INSERT INTO "effect_agent_canonical_records" VALUES('checkpoint-crash',1,'thread-created:checkpoint-crash','thread-created:checkpoint-crash','{"createdAt":"2026-10-05T01:09:48.716Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"ThreadCreated","agentId":"crash-checkpoint","definitions":{"agent":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","model":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tools":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},"recordId":"thread-created:checkpoint-crash","schemaVersion":1}');
INSERT INTO "effect_agent_canonical_records" VALUES('checkpoint-crash',2,'input:submission-01a1099c-1e29-7058-ae6b-921be7af7937','submission-input:submission-01a1099c-1e29-7058-ae6b-921be7af7937','{"createdAt":"2026-10-05T01:09:48.725Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"UserInputRecorded","input":{"question":"does accepted work survive a process kill?"},"kind":"user","runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","submissionId":"submission-01a1099c-1e29-7058-ae6b-921be7af7937"},"recordId":"input:submission-01a1099c-1e29-7058-ae6b-921be7af7937","schemaVersion":1}');
INSERT INTO "effect_agent_canonical_records" VALUES('checkpoint-crash',3,'run-start:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937','run-start:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937','{"createdAt":"2026-10-05T01:09:48.727Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"RunStarted","maxDurationMillis":30000,"policyAccountingVersion":1,"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937"},"recordId":"run-start:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","schemaVersion":1}');
INSERT INTO "effect_agent_canonical_records" VALUES('checkpoint-crash',4,'model-response:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1','turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1','{"createdAt":"2026-10-05T01:09:48.748Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"ModelResponseRecorded","inputTokens":100,"messages":{"content":[{"content":"Keep original checkpoint instructions.","options":{},"role":"system"},{"content":"{\"question\":\"does accepted work survive a process kill?\"}","options":{},"role":"user"},{"content":[{"id":"checkpoint-call-1","name":"search","options":{},"params":{"query":"checkpoint-read-1"},"providerExecuted":false,"type":"tool-call"}],"options":{},"role":"assistant"}]},"messagesDigest":"fa7f2343f0193a45c028ced635ec14d10cd819c0d35db98aa13b1ea3f85776ee","modelUsage":[{"costMicrousd":0,"inputTokens":{"cacheRead":0,"cacheWrite":0,"total":100,"uncached":100},"model":"crash-harness","outputTokens":{"reasoning":0,"text":10,"total":10},"pricingStatus":"unknown","provider":"scripted","purpose":"turn","usageStatus":"partial","webSearchCalls":0}],"outputTokens":10,"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","runScopedPrefixLength":2,"toolOperations":[{"executionClass":"readonly","executionKind":"ordinary","replay":"cdc2a046e08d84beb8ae1b6d4b3c3578d6f9f5c5fae56d4fd05665cf77ffd412","toolCallId":"checkpoint-call-1","toolName":"search"}],"turn":1,"turnId":"turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1"},"recordId":"model-response:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1","schemaVersion":1}');
INSERT INTO "effect_agent_canonical_records" VALUES('checkpoint-crash',5,'tool-settled:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1:checkpoint-call-1','turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1','{"createdAt":"2026-10-05T01:09:48.754Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"ToolCallSettled","isFailure":false,"result":{"available":true},"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","toolCallId":"checkpoint-call-1","toolName":"search"},"recordId":"tool-settled:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:1:checkpoint-call-1","schemaVersion":1}');
INSERT INTO "effect_agent_canonical_records" VALUES('checkpoint-crash',6,'model-response:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2','turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2','{"createdAt":"2026-10-05T01:09:48.757Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"ModelResponseRecorded","inputTokens":100,"messages":{"content":[{"content":[{"id":"checkpoint-call-2","name":"search","options":{},"params":{"query":"checkpoint-read-2"},"providerExecuted":false,"type":"tool-call"}],"options":{},"role":"assistant"}]},"messagesDigest":"34623417e140cf96e6e4b1956c067d3d7524d92c130d191fc16288b3129aef84","modelUsage":[{"costMicrousd":0,"inputTokens":{"cacheRead":0,"cacheWrite":0,"total":100,"uncached":100},"model":"crash-harness","outputTokens":{"reasoning":0,"text":10,"total":10},"pricingStatus":"unknown","provider":"scripted","purpose":"turn","usageStatus":"partial","webSearchCalls":0}],"outputTokens":10,"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","toolOperations":[{"executionClass":"readonly","executionKind":"ordinary","replay":"cdc2a046e08d84beb8ae1b6d4b3c3578d6f9f5c5fae56d4fd05665cf77ffd412","toolCallId":"checkpoint-call-2","toolName":"search"}],"turn":2,"turnId":"turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2"},"recordId":"model-response:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2","schemaVersion":1}');
INSERT INTO "effect_agent_canonical_records" VALUES('checkpoint-crash',7,'tool-settled:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2:checkpoint-call-2','turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2','{"createdAt":"2026-10-05T01:09:48.758Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"ToolCallSettled","isFailure":false,"result":{"available":true},"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","toolCallId":"checkpoint-call-2","toolName":"search"},"recordId":"tool-settled:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2:checkpoint-call-2","schemaVersion":1}');
INSERT INTO "effect_agent_canonical_records" VALUES('checkpoint-crash',8,'compaction:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:3:rollover','compaction:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:3:rollover','{"createdAt":"2026-10-05T01:09:48.763Z","deploymentId":"deployment-crash","family":"thread","payload":{"_tag":"CompactionCreated","coversThrough":7,"handoff":"Keep the checkpoint continuation.","kind":"rollover","runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","turn":3},"recordId":"compaction:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:3:rollover","schemaVersion":1}');
CREATE TABLE "effect_agent_checkpoints" (
      thread_id TEXT NOT NULL,
      through_sequence INTEGER NOT NULL,
      tail_digest TEXT NOT NULL,
      checkpoint_json TEXT NOT NULL,
      PRIMARY KEY (thread_id, through_sequence),
      FOREIGN KEY (thread_id)
        REFERENCES "effect_agent_threads"(thread_id)
        ON DELETE RESTRICT
    );
CREATE TABLE "effect_agent_child_reservations" (
      reservation_id TEXT PRIMARY KEY NOT NULL,
      parent_submission_id TEXT NOT NULL,
      parent_tool_call_id TEXT NOT NULL,
      child_submission_id TEXT,
      status TEXT NOT NULL,
      allocation_json TEXT NOT NULL,
      allocation_digest TEXT NOT NULL,
      accounting_json TEXT,
      reserved_at TEXT NOT NULL,
      release_began_at TEXT,
      released_at TEXT,
      UNIQUE (parent_submission_id, parent_tool_call_id),
      FOREIGN KEY (parent_submission_id)
        REFERENCES "effect_agent_submissions"(submission_id)
        ON DELETE RESTRICT
    );
CREATE TABLE "effect_agent_message_deliveries" (
      owner_thread_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      version INTEGER NOT NULL,
      state TEXT NOT NULL,
      deadline_at_millis INTEGER,
      record_json TEXT NOT NULL,
      PRIMARY KEY (owner_thread_id, message_id)
    );
CREATE TABLE "effect_agent_recovery_checkpoints" (
      thread_id TEXT PRIMARY KEY NOT NULL,
      through_sequence INTEGER NOT NULL,
      tail_digest TEXT NOT NULL,
      checkpoint_json TEXT NOT NULL,
      FOREIGN KEY (thread_id) REFERENCES "effect_agent_threads"(thread_id) ON DELETE RESTRICT
    );
INSERT INTO "effect_agent_recovery_checkpoints" VALUES('checkpoint-crash',8,'4441eb6584c9530b7b4788585c3845635625ec098f90c687c3e2c5888c18af57','{"schemaVersion":1,"threadId":"checkpoint-crash","throughSequence":8,"tailDigest":"4441eb6584c9530b7b4788585c3845635625ec098f90c687c3e2c5888c18af57","engineVersion":"effect-agent/recovery@5","agentDefinitionDigest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","modelDigest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","toolDigest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","state":{"state":{"schemaVersion":2,"policyAccountingVersion":1,"submissionId":"submission-01a1099c-1e29-7058-ae6b-921be7af7937","submissionIds":["submission-01a1099c-1e29-7058-ae6b-921be7af7937"],"seed":{"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","retiredToolCallIds":["checkpoint-call-1"],"throughSequence":5,"firstSequence":2,"committedTurns":1,"policyUsage":{"committedTurns":1,"toolCalls":1,"programmaticToolCalls":0,"consecutiveToolFailures":0,"finalizationUsed":false,"modelRestarts":0},"modelCalls":1,"unobservedModelCalls":0,"inputTokens":100,"outputTokens":10,"lastInputTokens":100,"lastOutputTokens":10,"costMicrousd":0,"summarizedModelUsage":{"modelCalls":1,"inputTokens":{"total":100,"uncached":100,"cacheRead":0,"cacheWrite":0},"outputTokens":{"total":10,"text":10,"reasoning":0},"webSearchCalls":0,"costMicrousd":0,"byModel":[{"provider":"scripted","model":"crash-harness","modelCalls":1,"inputTokens":{"total":100,"uncached":100,"cacheRead":0,"cacheWrite":0},"outputTokens":{"total":10,"text":10,"reasoning":0},"webSearchCalls":0,"costMicrousd":0}],"usageStatus":"partial","pricingStatus":"unknown"},"protectedContext":{"content":[{"options":{},"role":"system","content":"Keep original checkpoint instructions."},{"options":{},"role":"user","content":"{\"question\":\"does accepted work survive a process kill?\"}"}]},"contextWindowId":"context:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:3","frontier":{"sequence":5,"tag":"ToolCallSettled"},"compaction":{"threadId":"checkpoint-crash","batchId":"compaction:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:3:rollover","sequence":8,"offset":"effect-agent-sqlite@1:checkpoint-crash:8","record":{"recordId":"compaction:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:3:rollover","family":"thread","schemaVersion":1,"createdAt":"2026-10-05T01:09:48.763Z","deploymentId":"deployment-crash","payload":{"_tag":"CompactionCreated","runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","turn":3,"kind":"rollover","coversThrough":7,"handoff":"Keep the checkpoint continuation."}}}},"records":[{"threadId":"checkpoint-crash","batchId":"thread-created:checkpoint-crash","sequence":1,"offset":"effect-agent-sqlite@1:checkpoint-crash:1","record":{"recordId":"thread-created:checkpoint-crash","family":"thread","schemaVersion":1,"createdAt":"2026-10-05T01:09:48.716Z","deploymentId":"deployment-crash","payload":{"_tag":"ThreadCreated","agentId":"crash-checkpoint","definitions":{"agent":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","model":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tools":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}},{"threadId":"checkpoint-crash","batchId":"submission-input:submission-01a1099c-1e29-7058-ae6b-921be7af7937","sequence":2,"offset":"effect-agent-sqlite@1:checkpoint-crash:2","record":{"recordId":"input:submission-01a1099c-1e29-7058-ae6b-921be7af7937","family":"thread","schemaVersion":1,"createdAt":"2026-10-05T01:09:48.725Z","deploymentId":"deployment-crash","payload":{"_tag":"UserInputRecorded","submissionId":"submission-01a1099c-1e29-7058-ae6b-921be7af7937","kind":"user","runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","input":{"question":"does accepted work survive a process kill?"}}}},{"threadId":"checkpoint-crash","batchId":"run-start:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","sequence":3,"offset":"effect-agent-sqlite@1:checkpoint-crash:3","record":{"recordId":"run-start:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","family":"thread","schemaVersion":1,"createdAt":"2026-10-05T01:09:48.727Z","deploymentId":"deployment-crash","payload":{"_tag":"RunStarted","runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","policyAccountingVersion":1,"maxDurationMillis":30000}}},{"threadId":"checkpoint-crash","batchId":"turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2","sequence":6,"offset":"effect-agent-sqlite@1:checkpoint-crash:6","record":{"recordId":"model-response:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2","family":"thread","schemaVersion":1,"createdAt":"2026-10-05T01:09:48.757Z","deploymentId":"deployment-crash","payload":{"_tag":"ModelResponseRecorded","toolOperations":[{"toolCallId":"checkpoint-call-2","toolName":"search","executionClass":"readonly","executionKind":"ordinary","replay":"cdc2a046e08d84beb8ae1b6d4b3c3578d6f9f5c5fae56d4fd05665cf77ffd412"}],"runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","turnId":"turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2","turn":2,"messages":{"content":[{"content":[{"id":"checkpoint-call-2","name":"search","options":{},"params":{"query":"checkpoint-read-2"},"providerExecuted":false,"type":"tool-call"}],"options":{},"role":"assistant"}]},"messagesDigest":"34623417e140cf96e6e4b1956c067d3d7524d92c130d191fc16288b3129aef84","modelUsage":[{"provider":"scripted","model":"crash-harness","purpose":"turn","usageStatus":"partial","pricingStatus":"unknown","inputTokens":{"total":100,"uncached":100,"cacheRead":0,"cacheWrite":0},"outputTokens":{"total":10,"text":10,"reasoning":0},"webSearchCalls":0,"costMicrousd":0}],"inputTokens":100,"outputTokens":10}}},{"threadId":"checkpoint-crash","batchId":"turn:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2","sequence":7,"offset":"effect-agent-sqlite@1:checkpoint-crash:7","record":{"recordId":"tool-settled:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:2:checkpoint-call-2","family":"thread","schemaVersion":1,"createdAt":"2026-10-05T01:09:48.758Z","deploymentId":"deployment-crash","payload":{"_tag":"ToolCallSettled","runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","toolCallId":"checkpoint-call-2","toolName":"search","result":{"available":true},"isFailure":false}}},{"threadId":"checkpoint-crash","batchId":"compaction:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:3:rollover","sequence":8,"offset":"effect-agent-sqlite@1:checkpoint-crash:8","record":{"recordId":"compaction:run:submission-01a1099c-1e29-7058-ae6b-921be7af7937:3:rollover","family":"thread","schemaVersion":1,"createdAt":"2026-10-05T01:09:48.763Z","deploymentId":"deployment-crash","payload":{"_tag":"CompactionCreated","runId":"run:submission-01a1099c-1e29-7058-ae6b-921be7af7937","turn":3,"kind":"rollover","coversThrough":7,"handoff":"Keep the checkpoint continuation."}}}]},"digest":"c16f33b845f8fb72aa06b82ea6db06b01aba6de9aa4a36a7282ceb87364bf5bb"},"createdAt":"2026-10-05T01:09:48.763Z"}');
CREATE TABLE "effect_agent_schedules" (
      tenant_id TEXT NOT NULL,
      owner_id TEXT NOT NULL,
      schedule_id TEXT NOT NULL,
      deadline_at_millis INTEGER,
      record_json TEXT NOT NULL,
      PRIMARY KEY (tenant_id, owner_id, schedule_id)
    );
CREATE TABLE "effect_agent_submission_ownership" (
      submission_id TEXT PRIMARY KEY NOT NULL,
      attempt_id TEXT NOT NULL,
      ownership_token TEXT NOT NULL,
      producer_epoch INTEGER NOT NULL,
      owner_producer_id TEXT NOT NULL,
      lease_expires_at TEXT NOT NULL,
      FOREIGN KEY (submission_id)
        REFERENCES "effect_agent_submissions"(submission_id)
        ON DELETE RESTRICT
    );
INSERT INTO "effect_agent_submission_ownership" VALUES('submission-01a1099c-1e29-7058-ae6b-921be7af7937','attempt-01a1099c-1e2e-7693-983c-897ca64334d0','owner-01a1099c-1e2e-7985-8ee3-119e83207504',1,'producer-crash-child','2026-10-05T01:09:48.968Z');
CREATE TABLE "effect_agent_submissions" (
      submission_id TEXT PRIMARY KEY NOT NULL,
      thread_id TEXT NOT NULL,
      queue_sequence INTEGER NOT NULL,
      principal TEXT NOT NULL,
      idempotency_key TEXT NOT NULL,
      agent_id TEXT NOT NULL,
      agent_digests_json TEXT NOT NULL,
      deployment_id TEXT NOT NULL,
      input_json TEXT NOT NULL,
      input_digest TEXT NOT NULL,
      receipt_id TEXT NOT NULL,
      state TEXT NOT NULL,
      settled_outcome TEXT,
      settled_record_id TEXT,
      finalized_at TEXT,
      created_at TEXT NOT NULL,
      ready_at TEXT,
      input_applied_record_id TEXT,
      input_applied_sequence INTEGER,
      joined_host_submission_id TEXT,
      suspended_reason_json TEXT,
      suspended_at TEXT,
      unknown_reason TEXT,
      unknown_tool_call_ids_json TEXT,
      parent_submission_id TEXT,
      parent_tool_call_id TEXT,
      admission_group TEXT,
      admission_fence_json TEXT,
      worker_admission_json TEXT,
      message_admission_json TEXT,
      UNIQUE (thread_id, principal, idempotency_key),
      UNIQUE (thread_id, queue_sequence)
    );
INSERT INTO "effect_agent_submissions" VALUES('submission-01a1099c-1e29-7058-ae6b-921be7af7937','checkpoint-crash',1,'principal-crash','checkpoint-crash','crash-checkpoint','{"agent":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","model":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tools":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}','deployment-crash','{"question":"does accepted work survive a process kill?"}','a4e1f82d81f7543ca98da8e66aea764fb77f69272e5cbf07a7cde05755dc447c','receipt-01a1099c-1e29-79b2-b056-f02d3eb99c71','input-applied',NULL,NULL,NULL,'2026-10-05T01:09:48.714Z','2026-10-05T01:09:48.717Z','input:submission-01a1099c-1e29-7058-ae6b-921be7af7937',2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
CREATE TABLE "effect_agent_subscription_deliveries" (
      tenant_id TEXT NOT NULL,
      source_address TEXT NOT NULL,
      owner_id TEXT NOT NULL,
      subscription_id TEXT NOT NULL,
      event_id TEXT NOT NULL,
      delivery_key TEXT NOT NULL,
      state TEXT NOT NULL,
      next_attempt_at_millis INTEGER NOT NULL,
      record_json TEXT NOT NULL,
      PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id, event_id),
      UNIQUE (tenant_id, source_address, delivery_key)
    );
CREATE TABLE "effect_agent_subscription_events" (
      tenant_id TEXT NOT NULL,
      source_address TEXT NOT NULL,
      event_id TEXT NOT NULL,
      source_name TEXT NOT NULL,
      source_version TEXT NOT NULL,
      matching_key TEXT NOT NULL,
      payload_digest TEXT NOT NULL,
      cutoff INTEGER NOT NULL,
      cursor INTEGER NOT NULL,
      routing_complete INTEGER NOT NULL,
      tombstone INTEGER NOT NULL DEFAULT 0,
      next_attempt_at_millis INTEGER NOT NULL,
      record_json TEXT NOT NULL,
      PRIMARY KEY (tenant_id, source_address, event_id)
    );
CREATE TABLE "effect_agent_subscription_sequences" (
      tenant_id TEXT NOT NULL,
      source_address TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      event_scan_cursor TEXT NOT NULL,
      delivery_scan_cursor TEXT NOT NULL,
      recovery_scan_cursor INTEGER NOT NULL,
      PRIMARY KEY (tenant_id, source_address)
    );
CREATE TABLE "effect_agent_subscriptions" (
      tenant_id TEXT NOT NULL,
      source_address TEXT NOT NULL,
      owner_id TEXT NOT NULL,
      subscription_id TEXT NOT NULL,
      ordinal INTEGER NOT NULL,
      source_name TEXT NOT NULL,
      source_version TEXT NOT NULL,
      matching_key TEXT NOT NULL,
      state TEXT NOT NULL,
      expires_at_millis INTEGER,
      recovery_at_millis INTEGER,
      recovery_present INTEGER NOT NULL DEFAULT 0,
      record_json TEXT NOT NULL,
      PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id),
      UNIQUE (tenant_id, source_address, ordinal)
    );
CREATE TABLE "effect_agent_threads" (
      thread_id TEXT PRIMARY KEY NOT NULL,
      created_at TEXT NOT NULL,
      tail_sequence INTEGER NOT NULL,
      tail_digest TEXT NOT NULL,
      producer_epoch INTEGER NOT NULL
    );
INSERT INTO "effect_agent_threads" VALUES('checkpoint-crash','2026-10-05T01:09:48.715Z',8,'4441eb6584c9530b7b4788585c3845635625ec098f90c687c3e2c5888c18af57',1);
CREATE TABLE "effect_agent_unknown_resolutions" (
      submission_id TEXT NOT NULL,
      tool_call_id TEXT NOT NULL,
      author TEXT NOT NULL,
      reason TEXT NOT NULL,
      resolution_json TEXT NOT NULL,
      resolved_at TEXT NOT NULL,
      PRIMARY KEY (submission_id, tool_call_id),
      FOREIGN KEY (submission_id)
        REFERENCES "effect_agent_submissions"(submission_id)
        ON DELETE RESTRICT
    );
CREATE TABLE effect_agent_worker_stops (thread_id TEXT PRIMARY KEY NOT NULL, terminal TEXT);
CREATE TABLE "effect_sql_migrations" (
  migration_id integer PRIMARY KEY NOT NULL,
  created_at datetime NOT NULL DEFAULT current_timestamp,
  name VARCHAR(255) NOT NULL
);
INSERT INTO "effect_sql_migrations" VALUES(1,'2026-10-05 01:09:48','current_thread_storage');
CREATE INDEX effect_agent_canonical_records_batch
      ON "effect_agent_canonical_records" (thread_id, batch_id, sequence)
  ;
CREATE INDEX effect_agent_submissions_group
      ON "effect_agent_submissions" (thread_id, admission_group, state)
  ;
CREATE INDEX effect_agent_submissions_joined_host
      ON "effect_agent_submissions" (joined_host_submission_id)
  ;
CREATE INDEX effect_agent_submissions_parent
      ON "effect_agent_submissions" (parent_submission_id)
  ;
CREATE INDEX effect_agent_schedules_deadline
      ON "effect_agent_schedules" (deadline_at_millis, tenant_id, owner_id, schedule_id)
      WHERE deadline_at_millis IS NOT NULL
  ;
CREATE INDEX effect_agent_schedules_owner_deadline
      ON "effect_agent_schedules" (tenant_id, owner_id, deadline_at_millis, schedule_id)
      WHERE deadline_at_millis IS NOT NULL
  ;
CREATE INDEX effect_agent_subscriptions_owner ON "effect_agent_subscriptions" (tenant_id, source_address, owner_id, ordinal);
CREATE INDEX effect_agent_subscriptions_candidates ON "effect_agent_subscriptions" (tenant_id, source_address, source_name, source_version, matching_key, ordinal);
CREATE INDEX effect_agent_subscriptions_recovery ON "effect_agent_subscriptions" (tenant_id, source_address, recovery_at_millis, ordinal) WHERE recovery_at_millis IS NOT NULL;
CREATE INDEX effect_agent_subscription_events_pending ON "effect_agent_subscription_events" (tenant_id, source_address, routing_complete, next_attempt_at_millis, event_id);
CREATE INDEX effect_agent_subscription_deliveries_pending ON "effect_agent_subscription_deliveries" (tenant_id, source_address, state, next_attempt_at_millis, delivery_key);
CREATE INDEX effect_agent_subscription_deliveries_registration ON "effect_agent_subscription_deliveries" (tenant_id, source_address, owner_id, subscription_id, delivery_key);
CREATE INDEX effect_agent_message_deliveries_due
    ON "effect_agent_message_deliveries" (deadline_at_millis, owner_thread_id, message_id)
    WHERE deadline_at_millis IS NOT NULL
  ;
CREATE INDEX effect_agent_submissions_nonterminal ON "effect_agent_submissions" (thread_id, queue_sequence) WHERE state <> 'settled';
CREATE INDEX effect_agent_records_call ON "effect_agent_canonical_records"(thread_id, json_extract(record_json, '$.payload._tag'), json_extract(record_json, '$.payload.runId'), json_extract(record_json, '$.payload.toolCallId'));
CREATE INDEX effect_agent_records_run_input ON "effect_agent_canonical_records"(thread_id, json_extract(record_json, '$.payload.runId')) WHERE json_extract(record_json, '$.payload._tag') = 'UserInputRecorded' AND json_extract(record_json, '$.payload.kind') = 'user';
CREATE INDEX effect_agent_records_subtree ON "effect_agent_canonical_records"(thread_id, json_extract(record_json, '$.payload.sourceSubmissionId'), sequence) WHERE json_extract(record_json, '$.payload._tag') = 'SubtreeBudgetReserved';
CREATE INDEX effect_agent_records_worker_input ON "effect_agent_canonical_records"(thread_id, json_extract(record_json, '$.payload.admission.messageId')) WHERE json_extract(record_json, '$.payload._tag') = 'WorkerInputRequested';
CREATE INDEX effect_agent_message_deliveries_pending ON "effect_agent_message_deliveries"(owner_thread_id, message_id) WHERE state NOT IN ('processed', 'refused');
CREATE INDEX effect_agent_worker_starts ON effect_agent_message_deliveries(owner_thread_id,
    json_extract(record_json, '$.envelope.workerAdmission.origin.worker.delegationId'),
    json_extract(record_json, '$.envelope.workerAdmission.origin.worker.targetAgentId'), message_id)
    WHERE message_id = json_extract(record_json, '$.envelope.workerAdmission.origin.firstMessageId');
CREATE INDEX effect_agent_worker_pending ON effect_agent_message_deliveries(owner_thread_id,
    json_extract(record_json, '$.envelope.workerAdmission.origin.worker.threadId'), message_id)
    WHERE state IN ('pending', 'parked') AND json_extract(record_json, '$.receipt') IS NULL;
CREATE INDEX effect_agent_worker_execution ON effect_agent_canonical_records(thread_id,
    json_extract(record_json, '$.payload._tag'), sequence) WHERE json_extract(record_json, '$.payload.runId') IS NOT NULL;
COMMIT;
PRAGMA foreign_keys = ON;
PRAGMA user_version = 16;
