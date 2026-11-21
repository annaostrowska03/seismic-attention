"""Draw a small sample of raw Bluesky records for data/sample/ (about 1000 lines, fixed seed).

The sample keeps the raw format of data/raw/bsky and covers the edge cases of the processing
rules, so it can be used for demos and quick checks without the full data set. Every post gets
the first group that fits:

    noto         indexed in the first hour after the Noto earthquake (2024-01-01 07:10 UTC)
    duplicate    stored more than once in the raw files (all copies are kept)
    bad_created  createdAt missing, unparsable, outside the study period or > 5 min after indexedAt
    reindexed    indexed more than 1 day after createdAt
    unrelated    text contains an exclusion term
    reply        a reply
    non_english  languages declared and none of them is English
    other        everything else

The thresholds are the ones of the Spark rules (src/seismic_attention/preprocess/bluesky.py);
the groups only steer the sampling, they are not stored.

Usage (from the repo root):
    python scripts/make_bsky_sample.py                       # writes data/sample/bsky_sample.jsonl
    python scripts/make_bsky_sample.py --out some/file.jsonl
"""
import argparse
import json
import random
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))  # run without installing the package

from seismic_attention.config import PERIOD_END, PERIOD_START  # noqa: E402
from seismic_attention.ingest.keywords import load_keywords  # noqa: E402

RAW_DIR = ROOT / "data" / "raw" / "bsky"
OUT_FILE = ROOT / "data" / "sample" / "bsky_sample.jsonl"

NOTO_START = datetime(2024, 1, 1, 7, 10, tzinfo=timezone.utc)
CREATED_TOLERANCE = timedelta(minutes=5)
REINDEXED_AFTER = timedelta(days=1)
QUOTAS = {                      # number of posts per group (duplicates add their extra copies)
    "noto": 150, "duplicate": 60, "bad_created": 100, "reindexed": 100,
    "unrelated": 100, "reply": 100, "non_english": 150, "other": 190,
}


def parse_ts(value):
    """ISO 8601 text -> aware datetime in UTC, or None if it cannot be read."""
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def group_of(post, copies, period, exclude_terms):
    created, indexed = parse_ts(post.get("createdAt")), parse_ts(post.get("indexedAt"))
    text = (post.get("text") or "").lower()
    langs = post.get("langs") or []
    if indexed and NOTO_START <= indexed < NOTO_START + timedelta(hours=1):
        return "noto"
    if copies > 1:
        return "duplicate"
    if (created is None or indexed is None or not period[0] <= created < period[1]
            or created > indexed + CREATED_TOLERANCE):
        return "bad_created"
    if indexed - created > REINDEXED_AFTER:
        return "reindexed"
    if any(t in text for t in exclude_terms):
        return "unrelated"
    if post.get("reply"):
        return "reply"
    if langs and not any(lang.lower().startswith("en") for lang in langs):
        return "non_english"
    return "other"


def read_raw():
    """Yield (raw line, parsed post) for every readable line, in file order."""
    for path in sorted(RAW_DIR.glob("*.jsonl")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    post = json.loads(line)
                except ValueError:
                    continue
                if isinstance(post, dict) and post.get("uri"):
                    yield line.rstrip("\r\n"), post


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=OUT_FILE)
    ap.add_argument("--seed", type=int, default=20240101)
    args = ap.parse_args()

    period = (parse_ts(PERIOD_START + "T00:00:00Z"), parse_ts(PERIOD_END + "T00:00:00Z"))
    exclude_terms = load_keywords().exclude_terms
    rng = random.Random(args.seed)

    # pass 1: how many raw copies every post has
    copies = Counter(post["uri"] for _, post in read_raw())

    # pass 2: reservoir sample of uris per group (first copy of each post only)
    chosen = {g: [] for g in QUOTAS}
    seen_in_group = Counter()
    visited = set()
    for _, post in read_raw():
        uri = post["uri"]
        if uri in visited:
            continue
        visited.add(uri)
        g = group_of(post, copies[uri], period, exclude_terms)
        seen_in_group[g] += 1
        if len(chosen[g]) < QUOTAS[g]:
            chosen[g].append(uri)
        else:
            j = rng.randrange(seen_in_group[g])
            if j < QUOTAS[g]:
                chosen[g][j] = uri

    # pass 3: write every raw copy of the chosen posts, in file order
    selected = {uri for uris in chosen.values() for uri in uris}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    n_lines = 0
    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        for line, post in read_raw():
            if post["uri"] in selected:
                f.write(line + "\n")
                n_lines += 1

    for g in QUOTAS:
        print(f"{g:<12} sampled {len(chosen[g]):>4} of {seen_in_group[g]}")
    print(f"posts: {len(selected)}, lines written: {n_lines} -> {args.out}")


if __name__ == "__main__":
    main()
