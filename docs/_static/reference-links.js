// Old class/member bookmarks belong on their generated reference pages.
function forwardMovedReference() {
  const fragment = window.location.hash;
  if (!fragment || document.querySelector(":target")) return;

  const pages = JSON.parse(document.querySelector("#reference-pages")?.textContent || "{}");
  const references = [
    ...Array.from(document.querySelectorAll("table.autosummary td:first-child a.reference.internal"),
      (link) => [new URL(link.href).hash, link.href]),
    ...Object.entries(pages),
  ];
  for (const [prefix, href] of references) {
    if (prefix && (fragment === prefix || fragment.startsWith(`${prefix}.`))) {
      const destination = new URL(href, window.location.href);
      destination.hash = fragment;
      destination.search = window.location.search;
      window.location.replace(destination.href);
      return;
    }
  }
}

document.addEventListener("DOMContentLoaded", forwardMovedReference);
window.addEventListener("hashchange", forwardMovedReference);
