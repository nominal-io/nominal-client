import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { runInNewContext } from "node:vm";

const script = readFileSync(new URL("../_static/reference-links.js", import.meta.url), "utf8");
const resource = "https://docs.example.test/sdk/generated/sample_api.Resource/#sample_api.Resource";
const create = "https://docs.example.test/sdk/generated/sample_api.create/#sample_api.create";

function page(hash, hasTarget = false, links = [resource, create]) {
  const redirects = [];
  const window = new EventTarget();
  window.location = new URL(`https://docs.example.test/sdk/experimental/sample/?view=compact${hash}`);
  window.location.replace = (url) => redirects.push(url);
  const document = new EventTarget();
  document.querySelector = () => hasTarget ? {} : null;
  document.querySelectorAll = () => links.map((href) => ({ href }));
  runInNewContext(script, { window, document, URL });
  document.dispatchEvent(new Event("DOMContentLoaded"));
  return { window, redirects };
}

for (const fragment of ["sample_api.Resource", "sample_api.Resource.read", "sample_api.Resource.rid"]) {
  test(`old ${fragment} bookmark reaches its class page and keeps the fragment and query`, () => {
    const { redirects } = page(`#${fragment}`);
    assert.deepEqual(redirects, [`${resource.split("#")[0]}?view=compact#${fragment}`]);
  });
}

test("old function bookmark reaches its function page", () => {
  assert.deepEqual(page("#sample_api.create").redirects, [create.replace("#", "?view=compact#")]);
});

test("existing prose or data targets stay on their current page", () => {
  assert.deepEqual(page("#sample_api.Resource", true).redirects, []);
});

for (const fragment of ["", "#unknown", "#sample_api.Resourceful.read"]) {
  test(`unmatched fragment ${fragment || '(none)'} stays on the overview`, () => {
    assert.deepEqual(page(fragment).redirects, []);
  });
}

test("pages without summary tables stay unchanged", () => {
  assert.deepEqual(page("#sample_api.Resource.read", false, []).redirects, []);
});

test("an old member fragment selected after load is also forwarded", () => {
  const { window, redirects } = page("");
  window.location.hash = "#sample_api.Resource.read";
  window.dispatchEvent(new Event("hashchange"));
  assert.deepEqual(redirects, [resource.replace("#sample_api.Resource", "?view=compact#sample_api.Resource.read")]);
});
