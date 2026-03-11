export function getFetcherPresetMeta(metaByPreset, preset) {
  return metaByPreset[String(preset || "default")] || metaByPreset.default;
}
