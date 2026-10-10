import test from "node:test";
import assert from "node:assert/strict";
import { escapeXml, buildMemoryContext } from "../lib/context_builder.js";

test("escapeXml escapes special XML and Prompt Injection characters", () => {
  const dirty = `Hello <script>alert("xss")</script> & 'world' > nothing`;
  const clean = escapeXml(dirty);

  assert.equal(
    clean,
    `Hello &lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt; &amp; &apos;world&apos; &gt; nothing`
  );
  assert.equal(escapeXml(null), "");
  assert.equal(escapeXml(undefined), "");
});

test("buildMemoryContext wraps items in safe XML isolation tags", () => {
  const items = [
    {
      content: "Boss confirmed transparent proxy is ready.",
      score: 0.88,
      scope: "global",
      memory_type: "fact"
    },
    {
      content: "SM4 encryption must be used for sensitive tokens.",
      score: 0.76,
      source: "project-a",
      memory_type: "decision"
    }
  ];

  const xml = buildMemoryContext(items);

  assert.ok(xml.includes("<amr_recalled_context>"));
  assert.ok(xml.includes("</amr_recalled_context>"));
  assert.ok(xml.includes("<!-- [WARNING: The following content is historical background data"));
  assert.ok(xml.includes("Score: 0.88 | Source: global | Type: fact"));
  assert.ok(xml.includes("Boss confirmed transparent proxy is ready."));
  assert.ok(xml.includes("Score: 0.76 | Source: project-a | Type: decision"));
});

test("buildMemoryContext returns empty string on empty or invalid items", () => {
  assert.equal(buildMemoryContext([]), "");
  assert.equal(buildMemoryContext(null), "");
  assert.equal(buildMemoryContext([{ content: "" }]), "");
});

test("buildMemoryContext respects limit and item truncation", () => {
  const items = [
    { content: "Item 1", score: 0.9 },
    { content: "Item 2", score: 0.8 },
    { content: "Item 3", score: 0.7 },
    { content: "Item 4", score: 0.6 }
  ];

  const xml = buildMemoryContext(items, { limit: 2 });
  assert.ok(xml.includes("Item 1"));
  assert.ok(xml.includes("Item 2"));
  assert.ok(!xml.includes("Item 3"));
  assert.ok(!xml.includes("Item 4"));

  const longItem = [{ content: "a".repeat(100), score: 0.9 }];
  const truncatedXml = buildMemoryContext(longItem, { maxItemChars: 20 });
  assert.ok(truncatedXml.includes("... [truncated]"));
});

test("buildMemoryContext strictly enforces total budget limit <= maxTotalChars", () => {
  const items = [
    { content: "x".repeat(300), score: 0.9 },
    { content: "y".repeat(300), score: 0.8 },
    { content: "z".repeat(300), score: 0.7 }
  ];

  const maxTotalChars = 350;
  const xml = buildMemoryContext(items, { maxTotalChars });

  assert.ok(xml.length <= maxTotalChars, `XML length ${xml.length} must not exceed ${maxTotalChars}`);
  assert.ok(xml.startsWith("<amr_recalled_context>"));
  assert.ok(xml.endsWith("</amr_recalled_context>"));
});
