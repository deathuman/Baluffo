const OPS_FETCHER_METRIC_SECTION_DEFINITIONS = [
  {
    key: "runtime",
    title: "Runtime",
    description: "Latest run performance, yield, and source-cost signals."
  },
  {
    key: "failures",
    title: "Failures",
    description: "Fetcher failure counts, buckets, and source examples."
  },
  {
    key: "dedup",
    title: "Dedup Review",
    description: "Read-only gate, review-state, and blocker evidence before lifecycle UX."
  },
  {
    key: "sourceHealth",
    title: "Source Health",
    description: "Source reliability, zero-kept, fallback, and productivity signals."
  },
  {
    key: "sourcePolicy",
    title: "Source Policy Signals",
    description: "Provider coverage, static suppression, cleanup proposals, and review context."
  },
  {
    key: "diagnostics",
    title: "Diagnostics",
    description: "Supporting evidence that does not own an operator action queue."
  }
];

export function getOpsFetcherMetricSectionDefinitions() {
  return OPS_FETCHER_METRIC_SECTION_DEFINITIONS.map(section => ({ ...section }));
}

export function buildOpsFetcherMetricSections(sectionContentByKey = {}) {
  const content = sectionContentByKey && typeof sectionContentByKey === "object"
    ? sectionContentByKey
    : {};
  return getOpsFetcherMetricSectionDefinitions()
    .map(section => ({
      ...section,
      html: String(content[section.key] || "")
    }))
    .filter(section => section.html.trim());
}
