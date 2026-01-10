import datetime
import pathlib
import zipfile
from io import BytesIO
import requests

BASE_URL = "https://www.ezmoney.com.tw"
FUND_CODE = "49YTW"  # 00981A
INFO_URL = f"{BASE_URL}/ETF/Fund/Info?FundCode={FUND_CODE}"
EXPORT_URL = f"{BASE_URL}/ETF/Fund/AssetExcelNPOI?fundCode={FUND_CODE}"

def is_html(b: bytes) -> bool:
    head = b.lstrip()[:200].lower()
    return head.startswith(b"<!doctype") or head.startswith(b"<html")

def is_zip(b: bytes) -> bool:
    return b[:4] == b"PK\x03\x04"

def save_bytes(path: pathlib.Path, content: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)

def extract_first_xlsx_from_zip(zip_bytes: bytes) -> tuple[str, bytes] | None:
    with zipfile.ZipFile(BytesIO(zip_bytes)) as z:
        # 優先找 .xlsx
        xlsx_names = [n for n in z.namelist() if n.lower().endswith(".xlsx")]
        if not xlsx_names:
            return None
        # 取第一個（也可以改成挑最像 portfolio/asset 的檔名）
        name = xlsx_names[0]
        return name, z.read(name)

def download_and_save():
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Referer": INFO_URL,
    }

    # 1) warm up cookies
    session.get(INFO_URL, headers=headers, timeout=30).raise_for_status()

    # 2) download
    resp = session.get(EXPORT_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    content = resp.content

    today = datetime.date.today().strftime("%Y%m%d")
    data_dir = pathlib.Path("data") / "00981A"
    raw_path = data_dir / f"raw_{today}.bin"
    save_bytes(raw_path, content)

    # 3) detect / normalize
    if is_html(content):
        # 被擋回 HTML：直接把檔存起來方便 debug
        html_path = data_dir / f"blocked_{today}.html"
        save_bytes(html_path, content)
        raise RuntimeError(f"Download returned HTML (possibly blocked). Saved to {html_path}")

    if is_zip(content):
        # 可能是 xlsx（也是 zip）或 zip 包 xlsx
        extracted = extract_first_xlsx_from_zip(content)

        if extracted is not None:
            name, xlsx_bytes = extracted
            out_path = data_dir / f"00981A_portfolio_{today}.xlsx"
            save_bytes(out_path, xlsx_bytes)
            print(f"[OK] Extracted {name} -> {out_path}")
            return

        # 沒找到 xlsx，那就先存 zip 給你看內容
        zip_path = data_dir / f"00981A_{today}.zip"
        save_bytes(zip_path, content)
        raise RuntimeError(f"ZIP returned but no .xlsx inside. Saved to {zip_path}")

    # 其他格式：先存起來 debug
    unknown_path = data_dir / f"unknown_{today}.dat"
    save_bytes(unknown_path, content)
    raise RuntimeError(f"Unknown content type. Saved to {unknown_path}")

if __name__ == "__main__":
    download_and_save()