import test from "node:test";
import assert from "node:assert/strict";
import { classifyCompanyType, normalizeSector } from "../../../frontend/jobs/domain.js";

const WBD_BOARD_SOURCE = "static_source::static:listing_url:https://careers.wbd.com/global/en/wb-games-jobs";

// Company-in-own-URL corroboration is NOT game evidence: ATS hosts
// (greenhouse/lever/workable/...) and own-domain careers sites embed the
// company slug for every employer, so the corroboration carries no
// employer-specific information (it misclassified Apple/NVIDIA-class
// non-games rows as Game). Evidence:
// docs/snapshots/sector-signal-contamination-2026-09-06.md.

test("company-in-own-URL matches are not game evidence", () => {
  assert.equal(
    classifyCompanyType("Apple", "Wireless SoC Design Engineer", "google_sheets", "https://jobs.apple.com/en-us/details/200559444/wireless-soc-design-engineer"),
    "Tech"
  );
  assert.equal(
    classifyCompanyType("NVIDIA", "Senior Software Engineer, Architecture", "workday_sources", "https://nvidia.wd1.myworkdayjobs.com/en-US/NVIDIAExternalCareersSite/job/Senior-Software-Engineer_R1234567"),
    "Tech"
  );
  assert.equal(
    normalizeSector("Game", "Apple", "Wireless SoC Design Engineer", "google_sheets", "https://jobs.apple.com/en-us/details/200559444/wireless-soc-design-engineer"),
    "Tech"
  );
  assert.equal(
    normalizeSector("Tech", "Some Employer", "Backend Engineer", "lever_sources", "https://jobs.lever.co/some-employer/9f2c1a55"),
    "Tech"
  );
});

test("game keyword evidence is unaffected", () => {
  assert.equal(
    normalizeSector("Tech", "Studio Other", "Senior Gameplay Programmer", "google_sheets", "https://example.com/gameplay"),
    "Game"
  );
  // Game-industry employer names keep carrying Game without any URL corroboration.
  assert.equal(
    normalizeSector("Tech", "Riot Games", "Software Engineer", "google_sheets", "https://www.riotgames.com/en/work-with-us"),
    "Game"
  );
});

// GAME_KEYWORDS do not count in source/jobLink: board URLs carry a
// games-flavored path for whole-company sites, so the URL keyword
// misattributed every employer's rows (WBD, CNN, HBO Max, Disney, SciGames)
// as Game. Keyword evidence stays scoped to company/title.
test("board-URL keywords are not game evidence", () => {
  assert.equal(
    normalizeSector("Game", "CNN", "Software Engineer II (Site Reliability Engineer)", WBD_BOARD_SOURCE, "https://careers.wbd.com/global/en/job/R000105665/Software-Engineer-II-Site-Reliability-Engineer"),
    "Tech"
  );
  assert.equal(
    classifyCompanyType("CNN", "Software Engineer II (Site Reliability Engineer)", WBD_BOARD_SOURCE, "https://careers.wbd.com/global/en/job/R000105665/Software-Engineer-II-Site-Reliability-Engineer"),
    "Tech"
  );
  assert.equal(
    normalizeSector("Game", "Disney Experiences", "Senior Core Systems Engineer", "static_source::static:listing_url:https://www.disneycareers.com/en/search-jobs/game/391/1", "https://www.disneycareers.com/en/job/sweden/senior-core-systems-engineer/391/94802233360"),
    "Tech"
  );
  assert.equal(
    normalizeSector("Game", "Sglottery", "Software Engineer II", "workday_sources", "https://scientificgames.wd5.myworkdayjobs.com/en-US/SciPlayCareers/job/Software-Engineer-II_R-24110"),
    "Tech"
  );
  assert.equal(
    normalizeSector("Game", "Sonyglobal", "Principal Technical Program Manager", "google_sheets", "https://sonyglobal.wd1.myworkdayjobs.com/en-US/SonyGlobalCareers/job/Principal-Technical-Program-Manager_JR-118447?q=game"),
    "Tech"
  );
});

test("company/title keyword evidence survives the scoping", () => {
  assert.equal(
    normalizeSector("Tech", "WB Games", "Senior Gameplay Programmer", WBD_BOARD_SOURCE, "https://careers.wbd.com/global/en/job/R000105670/Senior-Gameplay-Programmer"),
    "Game"
  );
  assert.equal(
    normalizeSector("Tech", "Schell Games", "Software Engineer", "google_sheets", "https://www.schellgames.com/careers/software-engineer"),
    "Game"
  );
});

test("dedicated game source-family hints are unaffected", () => {
  assert.equal(
    normalizeSector("Tech", "Some Studio", "Producer", "gamejobs", "https://gamejobs.co/some-studio-producer"),
    "Game"
  );
});

test("provider provenance evidence is unaffected", () => {
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
});

// Curated employer-name hints (company field only) recover the
// URL-keyword-carried games employers lost to the keyword scoping: their rows
// ride shared aggregator sources (google_sheets) or static boards with no
// per-employer provenance, and their names carry no game token.
test("curated employer names recover URL-carried game companies", () => {
  assert.equal(
    normalizeSector("Tech", "Electronic Arts", "Anti-Cheat Engineer", "static_source::static:listing_url:https://careers.ea.com/careers", "https://jobs.ea.com/en_US/careers/JobDetail/Anti-Cheat-Engineer/212779"),
    "Game"
  );
  assert.equal(
    classifyCompanyType("Avalanchestudios", "Total Rewards Specialist", "google_sheets", "https://jobs.lever.co/avalanchestudios/72e4e6a4-f723-48ef-8d83-93e885ecd8a1"),
    "Game"
  );
  assert.equal(
    normalizeSector("Tech", "Metacore", "Talent Acquisition Partner", "google_sheets", "https://job-boards.eu.greenhouse.io/metacore/jobs/4793672101"),
    "Game"
  );
  assert.equal(
    normalizeSector("Tech", "playrix", "Junior QA Engineer (Manual)", "google_sheets", "https://playrix.com/job/open/qa/junior-qa-engineer-manual"),
    "Game"
  );
  assert.equal(
    normalizeSector("Tech", "Daybreak", "Accountant Intern", "google_sheets", "https://www.daybreakgames.com/careers?job=8468583002"),
    "Game"
  );
  assert.equal(
    normalizeSector("Tech", "Square Enix", "Japan", "static_source::static:listing_url:https://www.square-enix-games.com/en_us/careers", "https://www.jp.square-enix.com/recruit/career"),
    "Game"
  );
  assert.equal(
    normalizeSector("Tech", "Lightbulb Crew", "Ex Sanguis", "static_source::static:listing_url:https://lightbulbcrew.fr", "https://firesquid.games/games/ex-sanguis"),
    "Game"
  );
});

test("employer name hints are company-field only and never capture lookalikes", () => {
  // Name evidence never leaks from titles/sources/links. (Company avoids the
  // frontend's separate "studio"-token keyword branch so the assertion isolates
  // the employer-name-hint path.)
  assert.equal(
    normalizeSector("Game", "Bakery Collective", "Former Metacore producer", "google_sheets", "https://example.com/metacore"),
    "Tech"
  );
  // Short-name trap companies from the dataset: EA is matched via multi-token
  // hints only ("electronic arts", "ea sports", "ea create") — a bare "ea"
  // hint would capture these.
  assert.equal(
    normalizeSector("Game", "EACH1", "Payroll Officer", "google_sheets", "https://jobs.smartrecruiters.com/EACH1/744000112291310-payroll-officer"),
    "Tech"
  );
  assert.equal(
    normalizeSector("Game", "Eataly", "Line Cook", "google_sheets", "https://jobs.eataly.com/line-cook"),
    "Tech"
  );
});

test("multi-board static with a foreign provider studio loses provenance", () => {
  const bundle = [
    { source: "static_source::static:listing_url:https://careers.wbd.com/careers", adapter: "static", studio: "Warner Bros. Discovery" },
    { source: "greenhouse_boards", adapter: "greenhouse", studio: "WB Games" }
  ];
  assert.equal(
    classifyCompanyType("CNN", "Software Engineer", "static_source::static:listing_url:https://careers.wbd.com/careers", "https://careers.wbd.com/global/en/job/r0000878", bundle),
    "Tech"
  );
});

test("multi-board static with employer-consistent provider studios keeps provenance", () => {
  const bundle = [
    { source: "static_source::static:listing_url:https://sonysandiego.example/careers", adapter: "static", studio: "Sony Interactive Entertainment San Diego Studio" },
    { source: "greenhouse_boards", adapter: "greenhouse", studio: "PlayStation Global" }
  ];
  assert.equal(
    classifyCompanyType("PlayStation Global", "Software Engineer", "greenhouse_boards", "https://job-boards.greenhouse.io/sonyinteractiveentertainmentglobal/jobs/6178586004", bundle),
    "Game"
  );
});
