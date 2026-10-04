#!/usr/bin/env python3
"""Which GAME_KEYWORDS additions recover real roles without admitting business ones.

`looks_like_game_job` is a substring-any test over a deliberately narrow list: 19 entries, all
multi-token or unambiguous. It rejects real game roles -- CD Projekt Red's "Senior VFX Artist",
Ubisoft's "Senior Hard Surface Artist" -- while correctly rejecting "Logistics Director" and
"ADS MONETIZATION" at the same studios. So widening it is a precision/recall trade, not a
fix, and the only honest way to choose is to measure.

The corpus is the coverage audit's own output. 1,226 of the 2,092 catalogue roles missing from
registered boards are rejected by the classifier; the other 866 pass it and are lost to
fetching or extraction instead, so widening the keyword list cannot touch them and a change
here should not be sold as recovering all 2,092.

There are no gold labels -- the catalogue indexes some plainly non-game roles -- so this
reports, for each candidate token, how many rejected roles it would recover and how many
obviously-non-game roles it would also admit. A token is worth adding when the first number is
large, the second is zero, and the recovered sample reads as game work on inspection.

Usage:
  python tools/coverage_classifier_candidates.py --show 20
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs.game_detection import GAME_KEYWORDS, looks_like_game_job  # noqa: E402

# Craft-specific tokens: they name a discipline that only exists to make games, and none of
# them is a word a non-game department uses. Business roles are deliberately absent --
# "project manager", "producer", "accountant" are not game signals and admitting them would
# turn a game-jobs feed into a jobs board.
CANDIDATES: tuple[str, ...] = (
    # Art
    "2d artist",
    "3d artist",
    "3d generalist",
    "3d modeler",
    "character modeler",
    "concept artist",
    "environment artist",
    "environmental artist",
    "hard surface artist",
    "hard-surface artist",
    "vehicle artist",
    "prop artist",
    "creature artist",
    "matte painter",
    "lookdev artist",
    "illustrator",
    "art director",
    "technical director",
    "creative director",
    "ui/ux",
    "ui artist",
    "ux designer",
    "ui designer",
    # Programming and engineering
    "graphics programmer",
    "gameplay programmer",
    "gameplay engineer",
    "tools programmer",
    "engineer gameplay",
    "vfx",
    "visual effects",
    "shader programmer",
    # Design
    "level designer",
    "level design",
    "narrative designer",
    "world builder",
    "mission designer",
    "game designer",
    "systems designer",
    "combat designer",
    "economy designer",
    # Audio
    "sound designer",
    "audio designer",
    "audio director",
    "voice director",
    # QA and support that is game-specific
    "gameplay tester",
    "game tester",
    "qa analyst",
    # Brand and community, which for a games studio is the sector
    "community manager",
    "game producer",
    # Animation. A craft, not a department, and the listing is a games feed.
    "animator",
    "animation",
    # Non-English listings. Japanese studios publish role titles in Japanese, where no
    # ASCII keyword can match: Marvelous's ゲームデザイナー and Bandai Namco's
    # ゲームAIエンジニア are both real game roles the substring test cannot see.
    "ゲームデザイナー",
    "ゲームエンジニア",
    "ゲーム開発",
    "デザイナー",
    "エンジニア",
    "アート",
    # French listings, same problem: Ubisoft's "Animateur.trice Cinématique Sénior".
    "animateur",
    "concepteur",
    "développeur",
    "ingénieur",
)

# Roles that are plainly not game work and must stay out, whatever else is widened.
MUST_STAY_OUT: tuple[tuple[str, str], ...] = (
    ("Logistics Director", "Atlas Creative"),
    ("Senior Mobile Data Analyst/Scientist", "Big Red Button Enter"),
    ("Project Manager", "AppQuantum"),
    ("Producer", "Atlas Creative"),
    ("ADS MONETIZATION", "ABI Game Studio"),
    ("Open application", "Black Beach Studio"),
    ("General Application", "Blind Squirrel Enter"),
    ("Finance Business Partner", "Big Red Button Enter"),
    ("Office Coordinator", "Hangar 13"),
    ("Talent Acquisition Partner", "Various"),
    ("Facilities Manager", "Various"),
    ("Accountant", "Various"),
    ("HR Business Partner", "Various"),
    ("Product Manager", "Various"),
    ("Marketing Manager", "Various"),
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", default=str(_ROOT / "_out/coverage/classifier-corpus.json"))
    parser.add_argument("--show", type=int, default=20)
    args = parser.parse_args()

    corpus = json.loads(Path(args.corpus).read_text(encoding="utf-8"))
    rejected = [
        (str(title), str(company))
        for title, company in corpus["fn"]
        if not looks_like_game_job(title, company)
    ]
    print(f"catalogue roles missing from registered boards : {len(corpus['fn'])}")
    print(f"  of those, rejected by the classifier        : {len(rejected)}")
    print(f"  already passing (lost to fetching, not this) : {len(corpus['fn']) - len(rejected)}\n")

    print(f"candidate tokens ({len(CANDIDATES)}), current list has {len(GAME_KEYWORDS)}:")
    print(f"{'recovers':>9}{'admits':>7}  token")
    ranked: list[tuple[int, int, str]] = []
    for token in CANDIDATES:
        hits = [(t, c) for t, c in rejected if token in f"{t} {c}".lower()]
        admits = sum(1 for title, company in MUST_STAY_OUT if token in f"{title} {company}".lower())
        ranked.append((len(hits), admits, token))
    for recovers, admits, token in sorted(ranked, reverse=True):
        if not recovers:
            continue
        flag = "  <-- admits a business role" if admits else ""
        print(f"{recovers:>9}{admits:>7}  {token}{flag}")

    safe = [token for recovers, admits, token in ranked if admits == 0 and recovers > 0]
    unsafe = [token for _recovers, admits, token in ranked if admits > 0]
    recovered = {t for t, _c in rejected if any(tok in f"{t}".lower() for tok in safe)}

    print(f"\nsafe tokens: {len(safe)}   tokens that admit a business role: {len(unsafe)}")
    if unsafe:
        print(f"  rejected on precision: {', '.join(unsafe)}")
    print(f"\nrejected roles recovered by the safe set alone: {len(recovered)} of {len(rejected)}")

    recovered_pairs = [(t, c) for t, c in rejected if t in recovered]
    companies = Counter(c for _t, c in recovered_pairs)
    print(f"companies benefiting: {len(companies)}")
    print(f"top: {', '.join(f'{c} ({n})' for c, n in companies.most_common(8))}")

    print(f"\nsample of what the safe set would newly admit ({args.show}):")
    shown = 0
    for title, company in recovered_pairs:
        if shown >= args.show:
            break
        matched = [tok for tok in safe if tok in title.lower()]
        if not matched:
            continue
        print(f"   {title[:52]:<54} {company[:20]:<22} via {matched[0]!r}")
        shown += 1

    residual = [(t, c) for t, c in rejected if t not in recovered]
    print(f"\nstill rejected after widening: {len(residual)}")
    print("what they are -- the classifier is right about most of them:")
    for title, company in residual[:: max(1, len(residual) // 18)][:18]:
        print(f"   {title[:52]:<54} {company[:22]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
