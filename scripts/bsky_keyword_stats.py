"""Per-keyword statistics of the raw Bluesky posts, to review the keyword list and exclusion terms.

Usage (from the repo root):
    python scripts/bsky_keyword_stats.py
    python scripts/bsky_keyword_stats.py --samples 10

Posts are deduplicated by uri. A keyword "matches" when it is a case-insensitive substring of the
post text (the same rule as matched_keywords in Spark). For every keyword:
    posts     unique posts whose text contains the keyword (2024 / 2025 by daily file)
    only      posts where it is the only keyword in the text, i.e. what this keyword adds;
              a keyword inside a longer matched one ("quake" in "earthquake") does not count
    flagged   posts with the keyword that also contain an exclusion term
Example texts are drawn at random (fixed seed) from the "only" posts; posts with no keyword in
the text are listed as well. The report is printed and saved to
data/processed/bsky_keyword_stats.txt.
"""
import argparse
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))  # run without installing the package

from seismic_attention.ingest.keywords import load_keywords  # noqa: E402

RAW_DIR = ROOT / "data" / "raw" / "bsky"
OUT_FILE = ROOT / "data" / "processed" / "bsky_keyword_stats.txt"
NO_KEYWORD = "(no keyword in text)"


def one_line(text, width=150):
    text = " ".join((text or "").split())
    return text if len(text) <= width else text[:width - 3] + "..."


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--samples", type=int, default=5, help="example texts per keyword")
    ap.add_argument("--seed", type=int, default=2024)
    args = ap.parse_args()

    cfg = load_keywords()
    keywords = cfg.search_keywords
    rng = random.Random(args.seed)

    seen = set()
    posts, only, flagged = Counter(), Counter(), Counter()
    by_year = defaultdict(Counter)
    samples = defaultdict(list)
    unreadable = 0

    for path in sorted(RAW_DIR.glob("*.jsonl")):
        year = path.name[:4]
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    post = json.loads(line)
                    uri = post["uri"]
                except (ValueError, KeyError, TypeError):
                    unreadable += 1
                    continue
                if uri in seen:
                    continue
                seen.add(uri)
                text = (post.get("text") or "").lower()
                matched = [k for k in keywords if k.lower() in text] or [NO_KEYWORD]
                own = [k for k in matched
                       if not any(k != o and k.lower() in o.lower() for o in matched)]
                is_flagged = any(t in text for t in cfg.exclude_terms)
                for k in matched:
                    posts[k] += 1
                    by_year[k][year] += 1
                    flagged[k] += is_flagged
                if len(own) == 1:
                    k = own[0]
                    only[k] += 1
                    # reservoir sampling: every "only" post has the same chance to be shown
                    if len(samples[k]) < args.samples:
                        samples[k].append(post.get("text"))
                    else:
                        j = rng.randrange(only[k])
                        if j < args.samples:
                            samples[k][j] = post.get("text")

    # keyword in the last column: wide (CJK) and right-to-left (Arabic) scripts would shift
    # or reorder any column after it
    lines = [f"unique posts: {len(seen)}   unreadable lines: {unreadable}", "",
             f"{'posts':>9}{'2024':>9}{'2025':>9}{'only':>9}{'flagged':>9}   keyword"]
    for k in keywords + [NO_KEYWORD]:
        lines.append(f"{posts[k]:>9}{by_year[k]['2024']:>9}{by_year[k]['2025']:>9}"
                     f"{only[k]:>9}{flagged[k]:>9}   {k}")
    for k in keywords + [NO_KEYWORD]:
        lines += ["", f"--- {k}: examples of posts where it is the only keyword ---"]
        lines += [f"  - {one_line(t)}" for t in samples[k]] or ["  (none)"]

    report = "\n".join(lines)
    print(report)
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(report + "\n", encoding="utf-8")
    print(f"\nsaved to {OUT_FILE.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
