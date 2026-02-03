from __future__ import annotations

import typing
from typing import Any

import jpinfectpy.urls as urls


def test_url_confirmed_sex_2010() -> None:
    url = urls.url_confirmed(2010, "sex")
    assert url.endswith("H22/Syuukei/Syu_01_1.xlsx")


def test_url_confirmed_place_2006() -> None:
    url = urls.url_confirmed(2006, "place")
    assert url.endswith("H18/Syuukei/Syu_02_1.xls")


def test_url_bullet_en(monkeypatch: Any) -> None:
    def fake_head(url: str, config: Any) -> Any:
        class Resp:
            status_code = 200
            headers: typing.ClassVar = {"content-length": "100"}

        return Resp()

    monkeypatch.setattr(urls, "cached_head", fake_head)
    result = urls.url_bullet(2025, 1, lang="en")
    assert result == ["https://id-info.jihs.go.jp/surveillance/idwr/en/rapid/2025/01/zensu01.csv"]


def test_url_bullet_ja(monkeypatch: Any) -> None:
    def fake_head(url: str, config: Any) -> Any:
        class Resp:
            status_code = 200
            headers: typing.ClassVar = {"content-length": "100"}

        return Resp()

    monkeypatch.setattr(urls, "cached_head", fake_head)
    result = urls.url_bullet(2025, 11, lang="ja")
    assert result == [
        "https://id-info.jihs.go.jp/surveillance/idwr/jp/rapid/2025/11/2025-11-zensu.csv"
    ]
