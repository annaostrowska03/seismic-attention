from datetime import datetime

import backfill_gdelt
import backfill_usgs


def test_months_within_one_year():
    assert backfill_usgs.months("2026-09", "2026-11") == [(2026, 9), (2026, 10), (2026, 11)]


def test_months_across_new_year():
    assert backfill_usgs.months("2025-11", "2026-02") == [(2025, 11), (2025, 12), (2026, 1), (2026, 2)]


def test_months_single():
    assert backfill_usgs.months("2026-10", "2026-10") == [(2026, 10)]


def test_day_stamps():
    stamps = backfill_gdelt.day_stamps(datetime(2026, 10, 31))
    assert len(stamps) == 96
    assert stamps[0] == "20261031000000"
    assert stamps[1] == "20261031001500"
    assert stamps[-1] == "20261031234500"


def gkg_line(gkg_id, themes, locations=""):
    cols = [""] * 27
    cols[0], cols[1], cols[3], cols[4] = gkg_id, "20261031003000", "example.com", "https://example.com/a"
    cols[7], cols[9] = themes, locations
    return "\t".join(cols)


def test_parse_gkg_keeps_only_earthquake_articles():
    text = "\n".join([
        gkg_line("a-1", "NATURAL_DISASTER;NATURAL_DISASTER_EARTHQUAKE;", "1#Chile#CI#CI#-30#-71#CI"),
        gkg_line("a-2", "TAX_FNCACT;NATURAL_DISASTER_FLOOD;"),
        gkg_line("a-3", "NATURAL_DISASTER_EARTHQUAKES;"),
        "broken line",
        "",
    ])
    articles = backfill_gdelt.parse_gkg(text)
    assert [a["gkg_id"] for a in articles] == ["a-1", "a-3"]
    assert articles[0] == {
        "gkg_id": "a-1",
        "date": "20261031003000",
        "source": "example.com",
        "url": "https://example.com/a",
        "themes": "NATURAL_DISASTER;NATURAL_DISASTER_EARTHQUAKE;",
        "locations": "1#Chile#CI#CI#-30#-71#CI",
    }
