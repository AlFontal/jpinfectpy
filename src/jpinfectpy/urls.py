from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from .config import get_config
from .http import cached_head


def url_confirmed(year: int, type: Literal["sex", "place"] = "sex") -> str:
    """
    Get the URL for the confirmed cases Excel file.
    """
    if type == "sex":
        if 1999 <= year <= 2006:
            url_a = "https://idsc.niid.go.jp/idwr/CDROM/Kako/"
            url_b = f"H{year - 1988:02d}/Syuukei/Syu_01_1.xls"
        elif 2007 <= year <= 2010:
            url_a = "https://idsc.niid.go.jp/idwr/CDROM/Kako/"
            url_b = f"H{year - 1988:02d}/Syuukei/Syu_01_1.xlsx"
        elif 2011 <= year <= 2013:
            url_a = "https://id-info.jihs.go.jp/niid/images/idwr/ydata/"
            url_b = f"{year}/H{year - 1988:02d}-01-1.xlsx"
        elif 2014 <= year <= 2021:
            url_a = "https://id-info.jihs.go.jp/niid/images/idwr/ydata/"
            url_b = f"{year}/Syu_01_1.xlsx"
        else:
            url_a = "https://id-info.jihs.go.jp/niid/images/idwr/ydata/"
            url_b = f"{year}/Syu_01_1.xlsx"
    else:  # type == "place"
        if year <= 2000:
            raise ValueError("Year must be >= 2001 for place data.")
        if 2001 <= year <= 2006:
            url_a = "https://idsc.niid.go.jp/idwr/CDROM/Kako/"
            url_b = f"H{year - 1988:02d}/Syuukei/Syu_02_1.xls"
        elif 2007 <= year <= 2010:
            url_a = "https://idsc.niid.go.jp/idwr/CDROM/Kako/"
            url_b = f"H{year - 1988:02d}/Syuukei/Syu_02_1.xlsx"
        elif 2011 <= year <= 2013:
            url_a = "https://id-info.jihs.go.jp/niid/images/idwr/ydata/"
            url_b = f"{year}/H{year - 1988:02d}-02-1.xlsx"
        elif 2014 <= year <= 2021:
            url_a = "https://id-info.jihs.go.jp/niid/images/idwr/ydata/"
            url_b = f"{year}/Syu_02_1.xlsx"
        else:
            url_a = "https://id-info.jihs.go.jp/niid/images/idwr/ydata/"
            url_b = f"{year}/Syu_02_1.xlsx"

    return f"{url_a}{url_b}"


def url_bullet(
    year: int,
    week: int | Iterable[int] | None = None,
    lang: Literal["en", "ja"] = "en",
) -> list[str]:
    if year <= 2023:
        raise ValueError("Year must be > 2023 for bullet data.")

    if week is None:
        weeks = list(range(1, 54))
    elif isinstance(week, int):
        weeks = [week]
    else:
        weeks = list(week)

    weeks = [w for w in weeks if 1 <= w <= 53]
    if not weeks:
        raise ValueError("Week must be between 1 and 53.")

    urls: list[str] = []
    config = get_config()

    for w in weeks:
        if lang == "en":
            url_a = "https://id-info.jihs.go.jp/surveillance/idwr/en/rapid/"
            url_b = f"{year}/{w:02d}/zensu{w:02d}.csv"
        else:
            if year >= 2025 and w >= 11:
                url_a = "https://id-info.jihs.go.jp/surveillance/idwr/jp/rapid/"
            else:
                url_a = "https://id-info.jihs.go.jp/surveillance/idwr/rapid/"
            url_b = f"{year}/{w:d}/{year}-{w:02d}-zensu.csv"

        url = f"{url_a}{url_b}"
        try:
            resp = cached_head(url, config)
            if resp.status_code == 200:
                content_length = resp.headers.get("content-length", "0")
                if int(content_length) > 0:
                    urls.append(url)
        except Exception:
            continue

    return urls
