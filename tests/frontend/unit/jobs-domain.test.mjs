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

test("jobs domain hides placeholder country values from filter options", () => {
  assert.equal(isValidCountry("Unknown"), false);
  assert.equal(isValidCountry("N/A"), false);
  assert.equal(isValidCountry("Remote"), true);
  assert.equal(isValidCountry("Japan"), true);
  assert.equal(isValidCountry("England"), true);
  assert.equal(isValidCountry("EU & NA"), true);
  assert.equal(isValidCountry("Onsite"), false);
});

test("jobs domain accepts the supported country vocabulary", () => {
  for (const label of getSupportedCountryLabels()) {
    assert.notEqual(sanitizeLocationField(label, "country"), "");
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

test("jobs domain preserves multiple locations for filtering and display", () => {
  const rows = normalizeJobs([
    {
      title: "Systems & Tools Engineer",
      company: "Stellar Entertainment",
      city: "",
      country: "Unknown",
      locations: [
        { city: "", country: "Unknown" },
        { city: "Guildford", country: "UK" },
        { city: "Utrecht", country: "NL" }
      ]
    }
  ], {
    professionLabels: {},
    sanitizeUrl: value => value
  });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].city, "Guildford");
  assert.equal(rows[0].country, "UK");
  assert.equal(rows[0].locationSummary, "Guildford, UK | Utrecht, NL");
  assert.deepEqual(getJobLocationCities(rows[0]), ["Guildford", "Utrecht"]);
  assert.deepEqual(getJobLocationCountries(rows[0]), ["UK", "NL"]);
});

test("jobs domain blanks label placeholder locations and keeps meaningful country fallbacks", () => {
  const rows = normalizeJobs([
    {
      title: "Associate QA Coordinator United States",
      company: "IllFonic",
      city: "%LABEL_POSITION_TYPE_REMOTE_ANY%",
      country: "Unknown",
      locations: [
        { city: "%LABEL_POSITION_TYPE_REMOTE_ANY%", country: "Unknown" },
        { city: "", country: "US" }
      ]
    }
  ], {
    professionLabels: {},
    sanitizeUrl: value => value
  });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].city, "");
  assert.equal(rows[0].country, "US");
  assert.equal(rows[0].locationSummary, "US");
  assert.deepEqual(getJobLocationCities(rows[0]), []);
  assert.deepEqual(getJobLocationCountries(rows[0]), ["US"]);
});

test("jobs domain blanks role blob locations and keeps meaningful country fallbacks", () => {
  const rows = normalizeJobs([
    {
      title: "Lead Level Scripter Montréal CDI",
      company: "Don't Nod",
      city: "Administratif, Assistant, Gestion, RH...",
      country: "Unknown",
      locations: [
        { city: "Administratif, Assistant, Gestion, RH...", country: "Unknown" },
        { city: "Paris", country: "FR" }
      ]
    }
  ], {
    professionLabels: {},
    sanitizeUrl: value => value
  });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].city, "");
  assert.equal(rows[0].country, "FR");
  assert.equal(rows[0].locationSummary, "Paris, FR");
  assert.deepEqual(getJobLocationCities(rows[0]), ["Paris"]);
  assert.deepEqual(getJobLocationCountries(rows[0]), ["FR"]);
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

test("jobs domain maps freshness ages to expected score bands", () => {
  assert.ok(mapFreshnessAgeToScore(0) >= 0 && mapFreshnessAgeToScore(0) <= 15);
  assert.ok(mapFreshnessAgeToScore(5) >= 16 && mapFreshnessAgeToScore(5) <= 40);
  assert.ok(mapFreshnessAgeToScore(12) >= 41 && mapFreshnessAgeToScore(12) <= 70);
  assert.ok(mapFreshnessAgeToScore(45) >= 71 && mapFreshnessAgeToScore(45) <= 100);
});

test("jobs domain derives freshness from postedAt first, then fetchedAt fallback", () => {
  const nowMs = Date.parse("2026-03-08T00:00:00.000Z");
  const posted = deriveFreshness({ postedAt: "2026-03-06T00:00:00.000Z" }, { nowMs });
  assert.equal(posted.freshnessSource, "postedAt");
  assert.equal(posted.freshnessAgeDays, 2);
  assert.ok(posted.freshnessScore >= 16 && posted.freshnessScore <= 40);

  const fetched = deriveFreshness({
    postedAt: "",
    fetchedAt: "2026-03-01T00:00:00.000Z"
  }, { nowMs });
  assert.equal(fetched.freshnessSource, "fetchedAt");
  assert.equal(fetched.freshnessAgeDays, 7);
  assert.ok(fetched.freshnessScore >= 16 && fetched.freshnessScore <= 40);
});

test("jobs domain returns null freshness when timestamps are missing/invalid", () => {
  const freshness = deriveFreshness({ postedAt: "bad", fetchedAt: "" });
  assert.equal(freshness.freshnessScore, null);
  assert.equal(freshness.freshnessAgeDays, null);
  assert.equal(freshness.freshnessSource, "");

  const rows = normalizeJobs([{ title: "Artist", company: "Studio", postedAt: "bad" }], {
    professionLabels: {},
    sanitizeUrl: value => value,
    nowMs: Date.parse("2026-03-08T00:00:00.000Z")
  });
  assert.equal(rows[0].freshnessScore, null);
  assert.equal(rows[0].freshnessAgeDays, null);
  assert.equal(rows[0].freshnessSource, "");
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

test("jobs domain blanks semantic location noise but preserves valid locations", () => {
  assert.equal(
    sanitizeLocationField(
      "Remote, United States; San Francisco Area, United States Remote; New York City; Los Angeles",
      "city"
    ),
    ""
  );
  assert.equal(
    sanitizeLocationField(
      "キャリア登録 「キャリア登録」とは？ 当社に興味・関心を持たれた方にご自身のキャリア（職務経歴）を簡易登録いただくことで、適したポジションがある場合、人事担当者から個別にご案内させていただく仕組みです。",
      "city"
    ),
    ""
  );
  assert.equal(sanitizeLocationField("6,559 followers", "city"), "");
  assert.equal(sanitizeLocationField("1,012 open jobs", "city"), "");
  assert.equal(
    sanitizeLocationField(
      '--grid-gutter: calc(var(--sqs-mobile-site-gutter, 6vw) - 0.0px);',
      "city"
    ),
    ""
  );
  assert.equal(
    sanitizeLocationField("#1 city in the country for women ,", "city"),
    ""
  );
  assert.equal(sanitizeLocationField("2D Artist, Bombergrounds", "city"), "");
  assert.equal(sanitizeLocationField("2D Games Animator - Freelancing - Fully Remote", "city"), "");
  assert.equal(sanitizeLocationField("A Fast, Fun Quiz Game", "city"), "");
  assert.equal(sanitizeLocationField("Berlin / Hamburg", "city"), "");
  assert.equal(sanitizeLocationField("Cambridge / Hybrid", "city"), "");
  assert.equal(sanitizeLocationField("%LABEL_POSITION_TYPE_REMOTE_ANY%", "city"), "");
  assert.equal(sanitizeLocationField(".career-btn-primary {", "city"), "");
  assert.equal(sanitizeLocationField("document.addEventListener(\"DOMContentLoaded\", function () {", "city"), "");
  assert.equal(sanitizeLocationField("Learn how talent, purpose, and progress combine to create careers that change the world at our new Careers home .", "city"), "");
  assert.equal(sanitizeLocationField("31-621 Kraków, Poland", "city"), "");
  assert.equal(
    sanitizeLocationField("1401 21st ST # 5799, Sacramento, CA 95811 United States", "city"),
    ""
  );
  assert.equal(sanitizeLocationField("Tokyo", "city"), "Tokyo");
  assert.equal(sanitizeLocationField("Japan", "country"), "Japan");
  assert.equal(sanitizeLocationField("United States of America", "country"), "US");
  assert.equal(sanitizeLocationField("Türkiye", "country"), "TR");
  assert.equal(sanitizeLocationField("Côte d'Ivoire", "country"), "CI");
  assert.equal(sanitizeLocationField("Hybrid", "country"), "");
  assert.equal(isSemanticallyValidLocationValue("Montréal", "city"), true);
  assert.equal(isSemanticallyValidLocationValue("6,559 followers", "city"), false);

  const rows = normalizeJobs([{
    title: "Growth Marketing Intern",
    company: "Sleeper",
    city: "Remote, United States; San Francisco Area, United States Remote; New York City; Los Angeles",
    country: "United States",
  }], {
    professionLabels: {},
    sanitizeUrl: value => value
  });
  assert.equal(rows[0].city, "");
  assert.equal(rows[0].country, "US");
});

test("jobs domain rejects shared city-noise contract fragments", () => {
  assert.equal(CITY_NOISE_CONTRACT.proseFragments.includes("bachelor's degree"), true);
  assert.equal(CITY_NOISE_CONTRACT.sentencePrefixes.includes("learn"), true);
  assert.equal(CITY_NOISE_CONTRACT.placeholderFragments.includes("%label_"), true);
  assert.equal(CITY_NOISE_CONTRACT.knownJunkTokens.includes("????"), true);
  for (const token of ["any", "eu & na", "uk", "spontaneous application", "work & innovation"]) {
    assert.equal(CITY_NOISE_CONTRACT.knownJunkTokens.includes(token), true);
  }

  const cases = [
    "A bachelor's degree in digital communications",
    "If you are looking for Tokyo",
    "%LABEL_POSITION_TYPE_REMOTE_ANY%",
    "????",
    "144 million+ Downloads",
    "3 to UTC+1",
    "9mo",
    "All",
    "Inc."
  ];
  for (const value of cases) {
    assert.equal(sanitizeLocationField(value, "city"), "");
    assert.equal(isSemanticallyValidLocationValue(value, "city"), false);
  }
});

test("jobs domain promotes country labels out of city fields", () => {
  for (const [value, expectedCountry] of [["EU & NA", "EU & NA"], ["UK", "UK"]]) {
    assert.equal(sanitizeLocationField(value, "city"), "");
    assert.equal(sanitizeLocationField(value, "country"), expectedCountry);
  }

  const rows = normalizeJobs([{
    title: "Artist",
    company: "Studio",
    city: "UK",
    country: "Unknown",
  }], {
    professionLabels: {},
    sanitizeUrl: value => value
  });
  assert.equal(rows[0].city, "");
  assert.equal(rows[0].country, "UK");
  assert.equal(rows[0].locationSummary, "UK");
});

test("jobs domain rejects structural city noise values", () => {
  for (const value of ["2026", "3"]) {
    assert.equal(sanitizeLocationField(value, "city"), "");
    assert.equal(isSemanticallyValidLocationValue(value, "city"), false);
  }
});
