const PROFESSION_LABELS = {
  gameplay: "Gameplay Programmer",
  graphics: "Graphics Programmer",
  engine: "Engine Programmer",
  ai: "AI Programmer",
  tools: "Tools Programmer",
  "technical-artist": "Technical Artist",
  "technical-animator": "Technical Animator",
  "environment-artist": "Environment Artist",
  "character-artist": "Character Artist",
  rigging: "Rigging",
  "vfx-artist": "VFX Artist",
  "ui-ux-artist": "UI/UX Artist",
  "concept-artist": "Concept Artist",
  "3d-artist": "3D Artist",
  "art-director": "Art Director",
  designer: "Game Designer",
  animator: "Animator",
  other: "Other"
};

const QUICK_FILTERS = [
  { key: "remote", label: "Remote Only", type: "workType", value: "Remote", defaultVisible: true },
  { key: "hybrid", label: "Hybrid Only", type: "workType", value: "Hybrid", defaultVisible: false },
  { key: "onsite", label: "On-Site Only", type: "workType", value: "Onsite", defaultVisible: false },
  { key: "exclude-internship", label: "Exclude Internship", type: "flag", value: "excludeInternship", defaultVisible: true },
  { key: "sector-game", label: "Game Sector", type: "sector", value: "Game", defaultVisible: false },
  { key: "sector-tech", label: "Tech Sector", type: "sector", value: "Tech", defaultVisible: false },
  { key: "netherlands", label: "Netherlands", type: "country", value: "NL", defaultVisible: true },
  { key: "united-states", label: "United States", type: "country", value: "US", defaultVisible: false },
  { key: "united-kingdom", label: "United Kingdom", type: "country", value: "GB", defaultVisible: false },
  { key: "gameplay", label: PROFESSION_LABELS.gameplay, type: "profession", value: "gameplay", defaultVisible: false },
  { key: "graphics", label: PROFESSION_LABELS.graphics, type: "profession", value: "graphics", defaultVisible: false },
  { key: "engine", label: PROFESSION_LABELS.engine, type: "profession", value: "engine", defaultVisible: false },
  { key: "ai", label: PROFESSION_LABELS.ai, type: "profession", value: "ai", defaultVisible: false },
  { key: "tools", label: PROFESSION_LABELS.tools, type: "profession", value: "tools", defaultVisible: false },
  { key: "technical-artist", label: PROFESSION_LABELS["technical-artist"], type: "profession", value: "technical-artist", defaultVisible: true },
  { key: "technical-animator", label: PROFESSION_LABELS["technical-animator"], type: "profession", value: "technical-animator", defaultVisible: false },
  { key: "environment-artist", label: PROFESSION_LABELS["environment-artist"], type: "profession", value: "environment-artist", defaultVisible: false },
  { key: "character-artist", label: PROFESSION_LABELS["character-artist"], type: "profession", value: "character-artist", defaultVisible: false },
  { key: "rigging", label: PROFESSION_LABELS.rigging, type: "profession", value: "rigging", defaultVisible: false },
  { key: "vfx-artist", label: PROFESSION_LABELS["vfx-artist"], type: "profession", value: "vfx-artist", defaultVisible: false },
  { key: "ui-ux-artist", label: PROFESSION_LABELS["ui-ux-artist"], type: "profession", value: "ui-ux-artist", defaultVisible: false },
  { key: "concept-artist", label: PROFESSION_LABELS["concept-artist"], type: "profession", value: "concept-artist", defaultVisible: false },
  { key: "3d-artist", label: PROFESSION_LABELS["3d-artist"], type: "profession", value: "3d-artist", defaultVisible: false },
  { key: "art-director", label: PROFESSION_LABELS["art-director"], type: "profession", value: "art-director", defaultVisible: false },
  { key: "designer", label: PROFESSION_LABELS.designer, type: "profession", value: "designer", defaultVisible: false },
  { key: "animator", label: PROFESSION_LABELS.animator, type: "profession", value: "animator", defaultVisible: false },
  { key: "clear", label: "Clear Filters", type: "clear", defaultVisible: true }
];

const DEFAULT_FILTERS = {
  workType: "",
  countries: [],
  city: "",
  sector: "",
  profession: "",
  excludeInternship: false,
  search: "",
  sort: "relevance"
};

export const JobsStateModule = {
  PROFESSION_LABELS,
  QUICK_FILTERS,
  DEFAULT_FILTERS
};

export { PROFESSION_LABELS, QUICK_FILTERS, DEFAULT_FILTERS };
