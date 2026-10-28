// Show each glossary term's definition, from glossary-terms.js, in a hover box.
document.addEventListener("DOMContentLoaded", () => {
  for (const link of document.querySelectorAll('a.reference.internal[href*="#term-"]')) {
    const definition = window.glossaryTerms?.[decodeURIComponent(link.hash.slice(1))];
    if (definition) tippy(link, { content: definition, theme: "glossary" });
  }
});
