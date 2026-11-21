"""Keyword configuration for the Bluesky source.

The JSON file lives inside the package so that it is available both on the host and in the
Spark containers (which mount ``src/`` but not ``conf/``).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

DEFAULT_KEYWORDS_FILE = Path(__file__).with_name("bluesky_keywords.json")


@dataclass(frozen=True)
class KeywordConfig:
    search_keywords: List[str]
    exclude_terms: List[str]


def load_keywords(path: Optional[Path] = None) -> KeywordConfig:
    """Load search keywords and exclusion terms (exclusion terms are lower-cased)."""
    data = json.loads(Path(path or DEFAULT_KEYWORDS_FILE).read_text(encoding="utf-8"))
    return KeywordConfig(
        search_keywords=list(data["search_keywords"]),
        exclude_terms=[t.lower() for t in data.get("exclude_terms", [])],
    )
