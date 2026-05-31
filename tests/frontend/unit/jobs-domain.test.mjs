import test from "node:test";
import assert from "node:assert/strict";
import {
  detectWorkType,
  detectContractType,
  classifyCompanyType,
  mapProfession,
  normalizeJobs,
  normalizeSector,
  sanitizePublicText,
  sanitizeLocationField,
  isSemanticallyValidLocationValue,
  isValidCountry,
  getJobLocationCities,
  getJobLocationCountries,
  getJobKeyForJob,
  deriveFreshness,
  mapFreshnessAgeToScore,
  toJobSnapshot
} from "../../../frontend/jobs/domain.js";
import { CITY_NOISE_CONTRACT } from "../../../frontend/shared/data/city-noise.js";
import { getSupportedCountryLabels } from "../../../frontend/jobs/app/countries.js";

test("jobs domain detects work type and contract", () => {
  assert.equal(detectWorkType("fully remote"), "Remote");
  assert.equal(detectWorkType("hybrid"), "Hybrid");
  assert.equal(detectWorkType("office"), "Onsite");
  assert.equal(detectContractType("internship"), "Internship");
  assert.equal(detectContractType("full time"), "Full-time");
  assert.equal(detectContractType("fixed term"), "Temporary");
});

test("jobs domain validates country filters and supported vocabulary", () => {
  for (const [caseId, value, expected] of [
    ["unknown", "Unknown", false],
    ["na", "N/A", false],
    ["remote", "Remote", true],
    ["japan", "Japan", true],
    ["england", "England", true],
    ["eu-na", "EU & NA", true],
    ["onsite", "Onsite", false]
  ]) {
    assert.equal(isValidCountry(value), expected, caseId);
  }
  for (const label of getSupportedCountryLabels()) {
    assert.notEqual(sanitizeLocationField(label, "country"), "", label);
  }
});

test("jobs domain classifies company and normalizes jobs", () => {
  assert.equal(classifyCompanyType("Some Game Studio", ""), "Game");
  const rows = normalizeJobs([{ title: "Gameplay Engineer", company: "Foo", workType: "remote" }], {
    professionLabels: {},
    sanitizeUrl: value => value
  });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].workType, "Remote");
  assert.equal(rows[0].companyType, "Game");
});

test("jobs domain filters rows with empty normalized titles", () => {
  const rows = normalizeJobs([
    { title: "", company: "Blank Studio" },
    { title: "   ", company: "Whitespace Studio" },
    { title: '<div class="cb"><', company: "Markup Studio" },
    { title: "Animator", company: "Real Studio" }
  ], {
    professionLabels: {},
    sanitizeUrl: value => value
  });

  assert.equal(rows.length, 1);
  assert.equal(rows[0].title, "Animator");
  assert.equal(rows[0].company, "Real Studio");
  assert.deepEqual(normalizeJobs([{ title: " ", company: "Only Blank" }]), []);
});

test("jobs domain normalizes location fallbacks for display and filtering", () => {
  const cases = [
    ["multi-location-display", {
      title: "Systems & Tools Engineer",
      company: "Stellar Entertainment",
      city: "",
      country: "Unknown",
      locations: [
        { city: "", country: "Unknown" },
        { city: "Guildford", country: "UK" },
        { city: "Utrecht", country: "NL" }
      ]
    }, "Guildford", "UK", "Guildford, UK | Utrecht, NL", ["Guildford", "Utrecht"], ["UK", "NL"]],
    ["label-placeholder-country-fallback", {
      title: "Associate QA Coordinator United States",
      company: "IllFonic",
      city: "%LABEL_POSITION_TYPE_REMOTE_ANY%",
      country: "Unknown",
      locations: [
        { city: "%LABEL_POSITION_TYPE_REMOTE_ANY%", country: "Unknown" },
        { city: "", country: "US" }
      ]
    }, "", "US", "US", [], ["US"]],
    ["role-blob-country-fallback", {
      title: "Lead Level Scripter Montréal CDI",
      company: "Don't Nod",
      city: "Administratif, Assistant, Gestion, RH...",
      country: "Unknown",
      locations: [
        { city: "Administratif, Assistant, Gestion, RH...", country: "Unknown" },
        { city: "Paris", country: "FR" }
      ]
    }, "", "FR", "Paris, FR", ["Paris"], ["FR"]]
  ];

  for (const [caseId, input, city, country, summary, cities, countries] of cases) {
    const rows = normalizeJobs([input], { professionLabels: {}, sanitizeUrl: value => value });
    assert.equal(rows.length, 1, caseId);
    assert.equal(rows[0].city, city, caseId);
    assert.equal(rows[0].country, country, caseId);
    assert.equal(rows[0].locationSummary, summary, caseId);
    assert.deepEqual(getJobLocationCities(rows[0]), cities, caseId);
    assert.deepEqual(getJobLocationCountries(rows[0]), countries, caseId);
  }
});

test("jobs domain normalizes sector from positive game evidence", () => {
  assert.equal(classifyCompanyType("Trek", "Assembler/Bike Builder"), "Tech");
  assert.equal(
    classifyCompanyType(
      "Zynga",
      "Marketing Artist",
      "greenhouse_boards",
      "https://job-boards.greenhouse.io/zyngacareers/jobs/5835998004",
      [{ source: "greenhouse_boards", studio: "Zynga", adapter: "greenhouse" }]
    ),
    "Game"
  );
  assert.equal(
    classifyCompanyType(
      "Cloud Chamber",
      "Senior Gameplay Programmer",
      "greenhouse_boards",
      "https://job-boards.greenhouse.io/cloudchamberen/jobs/7655929003",
      [{ source: "greenhouse_boards", studio: "Cloud Chamber", adapter: "greenhouse" }]
    ),
    "Game"
  );
  assert.equal(
    normalizeSector("Game", "Trek", "Assembler/Bike Builder", "google_sheets", "https://trekbikes.com/jobs/assembler"),
    "Tech"
  );
  assert.equal(
    normalizeSector("Tech", "Studio Other", "Gameplay Programmer", "google_sheets", "https://example.com/gameplay"),
    "Game"
  );
  assert.equal(
    normalizeSector("Tech", "Gameloft", "Marketing Artist", "smartrecruiters_sources", "https://jobs.smartrecruiters.com/Gameloft/744000115751281"),
    "Game"
  );
  const rows = normalizeJobs(
    [
      { title: "Assembler/Bike Builder", company: "Trek", sector: "Game", source: "google_sheets", jobLink: "https://trekbikes.com/jobs/assembler" },
      { title: "Gameplay Programmer", company: "Studio Other", sector: "Tech", source: "google_sheets", jobLink: "https://example.com/gameplay" },
      { title: "Marketing Artist", company: "Zynga", sector: "Tech", source: "greenhouse_boards", jobLink: "https://job-boards.greenhouse.io/zyngacareers/jobs/5835998004", sourceBundle: [{ source: "greenhouse_boards", studio: "Zynga", adapter: "greenhouse" }] },
      { title: "Marketing Artist", company: "Gameloft", sector: "Tech", source: "smartrecruiters_sources", jobLink: "https://jobs.smartrecruiters.com/Gameloft/744000115751281" }
      ,
      { title: "Senior Gameplay Programmer", company: "Cloud Chamber", sector: "Tech", source: "google_sheets", jobLink: "https://example.com/cloud-chamber/senior-gameplay-programmer", sourceBundle: [{ source: "greenhouse_boards", studio: "Cloud Chamber", adapter: "greenhouse" }] }
    ],
    {
      professionLabels: {},
      sanitizeUrl: value => value
    }
  );
  assert.equal(rows[0].sector, "Tech");
  assert.equal(rows[1].sector, "Game");
  assert.equal(rows[2].sector, "Game");
  assert.equal(rows[3].sector, "Game");
  assert.equal(rows[4].sector, "Game");
  assert.equal(rows[0].companyType, "Tech");
  assert.equal(rows[2].companyType, "Game");
  assert.equal(rows[3].companyType, "Game");
  assert.equal(rows[4].companyType, "Game");
});

test("jobs domain maps technical director title synonyms", () => {
  assert.equal(mapProfession("Technical Director"), "technical-director");
  assert.equal(mapProfession("Associate Technical Director"), "technical-director");
  assert.equal(mapProfession("Senior Animation TD"), "technical-director");
  assert.equal(mapProfession("Pipeline TD"), "technical-director");
  assert.equal(mapProfession("TDengine Programmer"), "engine");

  const rows = normalizeJobs([{ title: "Technical Director - Tools", company: "Studio" }], {
    professionLabels: { "technical-director": "Technical Director" },
    sanitizeUrl: value => value
  });
  assert.equal(rows[0].profession, "technical-director");
});

test("jobs domain generates fallback key", () => {
  const key = getJobKeyForJob({ title: "A", company: "B", city: "C", country: "D" }, {});
  assert.match(key, /^job_/);
});

test("jobs domain derives freshness scores, sources, and invalid timestamp fallbacks", () => {
  for (const [caseId, ageDays, minScore, maxScore] of [
    ["fresh", 0, 0, 15],
    ["recent", 5, 16, 40],
    ["stale", 12, 41, 70],
    ["old", 45, 71, 100]
  ]) {
    const score = mapFreshnessAgeToScore(ageDays);
    assert.ok(score >= minScore && score <= maxScore, caseId);
  }

  const nowMs = Date.parse("2026-03-08T00:00:00.000Z");
  for (const [caseId, input, expectedSource, expectedAge] of [
    ["posted-at", { postedAt: "2026-03-06T00:00:00.000Z" }, "postedAt", 2],
    ["fetched-at", { postedAt: "", fetchedAt: "2026-03-01T00:00:00.000Z" }, "fetchedAt", 7]
  ]) {
    const freshness = deriveFreshness(input, { nowMs });
    assert.equal(freshness.freshnessSource, expectedSource, caseId);
    assert.equal(freshness.freshnessAgeDays, expectedAge, caseId);
    assert.ok(freshness.freshnessScore >= 16 && freshness.freshnessScore <= 40, caseId);
  }

  const invalid = deriveFreshness({ postedAt: "bad", fetchedAt: "" });
  assert.equal(invalid.freshnessScore, null, "invalid-direct-score");
  assert.equal(invalid.freshnessAgeDays, null, "invalid-direct-age");
  assert.equal(invalid.freshnessSource, "", "invalid-direct-source");

  const rows = normalizeJobs([{ title: "Artist", company: "Studio", postedAt: "bad" }], {
    professionLabels: {},
    sanitizeUrl: value => value,
    nowMs
  });
  assert.equal(rows[0].freshnessScore, null, "invalid-normalized-score");
  assert.equal(rows[0].freshnessAgeDays, null, "invalid-normalized-age");
  assert.equal(rows[0].freshnessSource, "", "invalid-normalized-source");
});

test("jobs domain normalizes lifecycle status and timestamps", () => {
  const rows = normalizeJobs([{
    title: "Animator",
    company: "Studio",
    status: "LIKELY_REMOVED",
    firstSeenAt: "2026-03-01T00:00:00.000Z",
    lastSeenAt: "2026-03-05T00:00:00.000Z",
    removedAt: "2026-03-06T00:00:00.000Z"
  }], {
    professionLabels: {},
    sanitizeUrl: value => value
  });
  assert.equal(rows[0].status, "likely_removed");
  assert.equal(rows[0].firstSeenAt, "2026-03-01T00:00:00.000Z");
  assert.equal(rows[0].lastSeenAt, "2026-03-05T00:00:00.000Z");
  assert.equal(rows[0].removedAt, "2026-03-06T00:00:00.000Z");
});

test("jobs domain sanitizes public text html fragments", () => {
  assert.equal(sanitizePublicText('<div class="location">Tokyo'), "Tokyo");
  assert.equal(sanitizePublicText("Japan</div>"), "Japan");
  assert.equal(sanitizePublicText('<div class="cb"><'), "");

  const rows = normalizeJobs([{
    title: '<div class="title">Technical Artist</div>',
    company: "Kojimaproductions",
    city: '<div class="location">Tokyo',
    country: "Japan</div>",
    sector: "<div>Game</div>"
  }], {
    professionLabels: {},
    sanitizeUrl: value => value
  });
  assert.equal(rows[0].title, "Technical Artist");
  assert.equal(rows[0].city, "Tokyo");
  assert.equal(rows[0].country, "Japan");
  assert.equal(rows[0].sector, "Game");
});

test("jobs domain keeps snapshot-derived locations and summary sanitized", () => {
  const snapshot = toJobSnapshot({
    title: "Gameplay Programmer",
    company: "Studio",
    city: "Berlin / Hamburg",
    country: "Onsite",
    locationSummary: "Should not be trusted",
    locations: [
      { city: "Tokyo", country: "Japan" },
      { city: "Guildford", country: "England" },
      { city: "", country: "Hybrid" }
    ],
    jobLink: "https://example.com/job"
  }, {
    sanitizeUrl: value => value
  });

  assert.equal(snapshot.city, "");
  assert.equal(snapshot.country, "");
  assert.deepEqual(snapshot.locations, [
    { city: "Tokyo", country: "Japan" },
    { city: "Guildford", country: "England" }
  ]);
  assert.equal(snapshot.locationSummary, "Tokyo, Japan | Guildford, England");
});

test("jobs domain applies city-noise, structural-noise, and country-promotion contracts", () => {
  const blankCityValues = [
    "Remote, United States; San Francisco Area, United States Remote; New York City; Los Angeles",
    "キャリア登録 「キャリア登録」とは？ 当社に興味・関心を持たれた方にご自身のキャリア（職務経歴）を簡易登録いただくことで、適したポジションがある場合、人事担当者から個別にご案内させていただく仕組みです。",
    "6,559 followers",
    "1,012 open jobs",
    "--grid-gutter: calc(var(--sqs-mobile-site-gutter, 6vw) - 0.0px);",
    "#1 city in the country for women ,",
    "2D Artist, Bombergrounds",
    "2D Games Animator - Freelancing - Fully Remote",
    "A Fast, Fun Quiz Game",
    "Berlin / Hamburg",
    "Cambridge / Hybrid",
    "%LABEL_POSITION_TYPE_REMOTE_ANY%",
    ".career-btn-primary {",
    "document.addEventListener(\"DOMContentLoaded\", function () {",
    "Learn how talent, purpose, and progress combine to create careers that change the world at our new Careers home .",
    "31-621 Kraków, Poland",
    "1401 21st ST # 5799, Sacramento, CA 95811 United States"
  ];
  for (const value of blankCityValues) {
    assert.equal(sanitizeLocationField(value, "city"), "", value);
  }

  assert.equal(sanitizeLocationField("Tokyo", "city"), "Tokyo");
  for (const [caseId, value, expected] of [
    ["japan", "Japan", "Japan"],
    ["usa", "United States of America", "US"],
    ["turkiye", "Türkiye", "TR"],
    ["cote-divoire", "Côte d'Ivoire", "CI"],
    ["hybrid", "Hybrid", ""]
  ]) {
    assert.equal(sanitizeLocationField(value, "country"), expected, caseId);
  }
  assert.equal(isSemanticallyValidLocationValue("Montréal", "city"), true);
  assert.equal(isSemanticallyValidLocationValue("6,559 followers", "city"), false);

  const rows = normalizeJobs([{
    title: "Growth Marketing Intern",
    company: "Sleeper",
    city: "Remote, United States; San Francisco Area, United States Remote; New York City; Los Angeles",
    country: "United States",
  }], { professionLabels: {}, sanitizeUrl: value => value });
  assert.equal(rows[0].city, "");
  assert.equal(rows[0].country, "US");

  assert.equal(CITY_NOISE_CONTRACT.proseFragments.includes("bachelor's degree"), true);
  assert.equal(CITY_NOISE_CONTRACT.sentencePrefixes.includes("learn"), true);
  assert.equal(CITY_NOISE_CONTRACT.placeholderFragments.includes("%label_"), true);
  assert.equal(CITY_NOISE_CONTRACT.knownJunkTokens.includes("????"), true);
  for (const token of ["any", "eu & na", "uk", "spontaneous application", "work & innovation"]) {
    assert.equal(CITY_NOISE_CONTRACT.knownJunkTokens.includes(token), true, token);
  }

  for (const value of [
    "A bachelor's degree in digital communications",
    "If you are looking for Tokyo",
    "%LABEL_POSITION_TYPE_REMOTE_ANY%",
    "????",
    "144 million+ Downloads",
    "3 to UTC+1",
    "9mo",
    "All",
    "Inc.",
    "2026",
    "3"
  ]) {
    assert.equal(sanitizeLocationField(value, "city"), "", value);
    assert.equal(isSemanticallyValidLocationValue(value, "city"), false, value);
  }

  for (const [value, expectedCountry] of [["EU & NA", "EU & NA"], ["UK", "UK"]]) {
    assert.equal(sanitizeLocationField(value, "city"), "", value);
    assert.equal(sanitizeLocationField(value, "country"), expectedCountry, value);
  }

  const countryRows = normalizeJobs([{
    title: "Artist",
    company: "Studio",
    city: "UK",
    country: "Unknown",
  }], { professionLabels: {}, sanitizeUrl: value => value });
  assert.equal(countryRows[0].city, "");
  assert.equal(countryRows[0].country, "UK");
  assert.equal(countryRows[0].locationSummary, "UK");
});
