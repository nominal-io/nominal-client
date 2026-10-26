// Old class/member bookmarks belong on the detail pages linked by native autosummary tables.
function forwardMovedReference() {
  const fragment = window.location.hash;
  if (!fragment || document.querySelector(":target")) return;

  for (const link of document.querySelectorAll("table.autosummary td:first-child a.reference.internal")) {
    const destination = new URL(link.href);
    if (destination.hash && (fragment === destination.hash || fragment.startsWith(`${destination.hash}.`))) {
      destination.hash = fragment;
      destination.search = window.location.search;
      window.location.replace(destination.href);
      return;
    }
  }
}

document.addEventListener("DOMContentLoaded", forwardMovedReference);
window.addEventListener("hashchange", forwardMovedReference);
