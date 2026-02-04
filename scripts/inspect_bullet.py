from pathlib import Path
from tempfile import TemporaryDirectory

from jpinfectpy.io import _read_bullet_pl, download_urls
from jpinfectpy.urls import url_bullet

# 1. Get URL for 2024 Week 1
urls = url_bullet(2024, 1)
if not urls:
    print("No URLs found for 2024 Week 1")
    exit()

print(f"URL: {urls[0]}")

from jpinfectpy.config import get_config

config = get_config()

# 2. Download
with TemporaryDirectory() as tmp:
    tmp_path = Path(tmp)
    files = download_urls(urls, tmp_path, config)

    # 3. Read
    if files:
        df = _read_bullet_pl(files[0])
        print("Columns:", df.columns)
        print(df.head())

        # Check for age-like columns in the raw CSV before renaming
        # Just read text lines
        with open(files[0], encoding="shift_jis", errors="replace") as f:
            for i in range(10):
                print(f"Line {i}: {f.readline().strip()}")
