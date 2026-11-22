import io
import zipfile
from datetime import datetime, date

import pytest

import backfill_gdelt
import backfill_usgs


def test_month_ranges_across_new_year():
    assert backfill_usgs.month_ranges(date(2025, 11, 1), date(2026, 2, 1)) == [
        (date(2025, 11, 1), date(2025, 12, 1)),
        (date(2025, 12, 1), date(2026, 1, 1)),
        (date(2026, 1, 1), date(2026, 2, 1)),
    ]


def test_month_ranges_part_of_month():
    assert backfill_usgs.month_ranges(date(2025, 3, 10), date(2025, 4, 5)) == [
        (date(2025, 3, 10), date(2025, 4, 1)),
        (date(2025, 4, 1), date(2025, 4, 5)),
    ]


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


class FakeResponse:
    def __init__(self, status_code, content=b""):
        self.status_code = status_code
        self.content = content

    def raise_for_status(self):
        pass


def zipped(text):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("file.gkg.csv", text)
    return buffer.getvalue()


@pytest.mark.parametrize("response, status, n_articles", [
    (FakeResponse(200, zipped(gkg_line("a-1", "NATURAL_DISASTER_EARTHQUAKE;"))), "ok", 1),
    (FakeResponse(404), "missing", 0),
    (FakeResponse(200, b"not a zip"), "broken", 0),
])
def test_fetch_file_status(monkeypatch, response, status, n_articles):
    monkeypatch.setattr(backfill_gdelt.requests, "get", lambda url, timeout: response)
    stamp, result, articles = backfill_gdelt.fetch_file("20261031003000")
    assert (result, len(articles)) == (status, n_articles)
