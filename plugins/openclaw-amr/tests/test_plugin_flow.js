import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { plugin, isHeartbeatOrNoise } from "../index.js";
import { encodeFrame, FrameDecoder } from "../lib/uds_client.js";

test("isHeartbeatOrNoise detects noise queries", () => {
  assert.equal(isHeartbeatOrNoise(""), true);
  assert.equal(isHeartbeatOrNoise("   "), true);
  assert.equal(isHeartbeatOrNoise("HEARTBEAT"), true);
  assert.equal(isHeartbeatOrNoise("heartbeat_check"), true);
  assert.equal(isHeartbeatOrNoise("PING"), true);
  assert.equal(isHeartbeatOrNoise("ping"), true);
  assert.equal(isHeartbeatOrNoise("How does AMR work?"), false);
});

test("Plugin lifecycle: full flow of prefetch and session ingest", async () => {
  const tmpSock = path.join(os.tmpdir(), `test_flow_${Date.now()}_${Math.random().toString(36).slice(2)}.sock`);
  if (fs.existsSync(tmpSock)) fs.unlinkSync(tmpSock);

  const receivedRequests = [];
  const server = net.createServer((socket) => {
    const decoder = new FrameDecoder();
    socket.on("data", (chunk) => {
      const frames = decoder.feed(chunk);
      for (const f of frames) {
        const req = JSON.parse(f.toString("utf8"));
        receivedRequests.push(req);

        if (req.method === "memory.search") {
          const resp = {
            jsonrpc: "2.0",
            id: req.id,
            result: {
              results: [
                {
                  content: "System architecture requires pure standard library in plugins.",
                  score: 0.92,
                  scope: "global",
                  memory_type: "decision"
                }
              ]
            }
          };
          socket.write(encodeFrame(resp));
        }
      }
    });
  });

  await new Promise((r) => server.listen(tmpSock, r));

  try {
    const registeredHooks = {};
    const mockApi = {
      pluginConfig: {
        socketPath: tmpSock,
        deadlineMs: 300,
        scoreThreshold: 0.7,
      },
      logger: {
        info: () => {},
        debug: () => {},
        warn: () => {},
        error: () => {},
      },
      registerHook: (name, handler) => {
        registeredHooks[name] = handler;
      }
    };

    // 1. Register plugin
    plugin.register(mockApi);
    assert.ok(registeredHooks["before_prompt_build"], "before_prompt_build must be registered");
    assert.ok(registeredHooks["agent_end"], "agent_end must be registered");

    // 2. Trigger before_prompt_build (Prefetch)
    const promptEvent = {
      currentUserMessage: "What are the rules for plugin development?",
      prompt: "What are the rules for plugin development?",
      messages: []
    };
    const agentCtx = {
      sessionId: "test-sess-001",
      activeProjectKeys: ["project-amr"]
    };

    const promptResult = await registeredHooks["before_prompt_build"](promptEvent, agentCtx);
    assert.ok(promptResult, "Expected hook result from prefetch");
    assert.ok(promptResult.prependSystemContext, "Expected prependSystemContext to be set");
    assert.ok(promptResult.prependSystemContext.includes("<amr_recalled_context>"));
    assert.ok(promptResult.prependSystemContext.includes("pure standard library in plugins"));

    // 3. Trigger agent_end (Ingest)
    const endEvent = {
      runId: "run-001",
      messages: [
        { role: "user", content: "What are the rules for plugin development?" },
        { role: "assistant", content: "Pure standard library, zero VRAM overhead." }
      ],
      success: true
    };

    await registeredHooks["agent_end"](endEvent, agentCtx);

    // Wait short time for async notify
    await new Promise((r) => setTimeout(r, 100));

    // Verify received calls on mock UDS
    const searchCall = receivedRequests.find((r) => r.method === "memory.search");
    const ingestCall = receivedRequests.find((r) => r.method === "session.ingest");

    assert.ok(searchCall, "memory.search should be called");
    assert.equal(searchCall.params.query, "What are the rules for plugin development?");
    assert.equal(searchCall.params.project_id, "project-amr");

    assert.ok(ingestCall, "session.ingest should be called via notify");
    assert.equal(ingestCall.params.session_id, "test-sess-001");
    assert.equal(ingestCall.params.agent_id, "openclaw");
    assert.equal(ingestCall.params.messages.length, 2);
    assert.equal(ingestCall.params.messages[0].sequence, 1);
    assert.equal(ingestCall.params.messages[1].sequence, 2);
  } finally {
    server.close();
    if (fs.existsSync(tmpSock)) fs.unlinkSync(tmpSock);
  }
});

test("Plugin lifecycle: Fail-open when AMR server is completely offline", async () => {
  const nonexistentSock = "/tmp/non_existent_amr_offline.sock";

  const registeredHooks = {};
  const mockApi = {
    pluginConfig: {
      socketPath: nonexistentSock,
      deadlineMs: 50,
    },
    logger: {
      info: () => {},
      debug: () => {},
      warn: () => {},
      error: () => {},
    },
    registerHook: (name, handler) => {
      registeredHooks[name] = handler;
    }
  };

  plugin.register(mockApi);

  // When server is offline, before_prompt_build should resolve cleanly without throwing
  const result = await registeredHooks["before_prompt_build"](
    { currentUserMessage: "Hello while server is dead" },
    { sessionId: "sess-dead" }
  );

  assert.equal(result, undefined, "Fail-open should return undefined gracefully");

  // Ingest should also not throw
  assert.doesNotThrow(() => {
    registeredHooks["agent_end"](
      { messages: [{ role: "user", content: "hi" }] },
      { sessionId: "sess-dead" }
    );
  });
});
