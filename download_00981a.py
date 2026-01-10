import datetime
import pathlib
import zipfile
import io
import requests

BASE_URL = "https://www.ezmoney.com.tw"
FUND_CODE = "49YTW"  # 00981A 的 fundCode

INFO_URL = f"{BASE_URL}/ETF/Fund/Info?FundCode={FUND_CODE}"
EXPORT_URL = f"{BASE_URL}/ETF/Fund/AssetExcelNPOI?fundCode={FUND_CODE}"


def looks_like_zip(data: bytes) -> bool:
    return len(data) >= 4 and data[:2] == b"PK"


def looks_like_xlsx(data: bytes) -> bool:
    # xlsx 本質也是 zip，所以只用 PK 不能分辨；但 content-type 會比較準
    return looks_like_zip(data)


def looks_like_html(data: bytes) -> bool:
    head = data[:200].lstrip().lower()
    return head.startswith(b"<!doctype html") or head.startswith(b"<html") or b"<html" in head


def download_and_save():
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0 Safari/537.36"
        )
    }

    # output dir
    data_dir = pathlib.Path("data") / "00981A"
    data_dir.mkdir(parents=True, exist_ok=True)

    # date tag
    today = datetime.date.today().strftime("%Y%m%d")

    # 1) open info page to set cookies
    resp_info = session.get(INFO_URL, headers=headers, timeout=30)
    resp_info.raise_for_status()
    print("[INFO] Opened INFO page OK (cookie/session ready)")

    # 2) download
    resp = session.get(EXPORT_URL, headers=headers, timeout=60)
    resp.raise_for_status()

    ctype = (resp.headers.get("Content-Type") or "").lower()
    print(f"[INFO] Download status={resp.status_code} content-type={ctype}")
    print(f"[INFO] Content-Length={resp.headers.get('Content-Length')} bytes={len(resp.content)}")
    print(f"[INFO] First 16 bytes={resp.content[:16]!r}")

    content = resp.content

    # 3) decide save format
    if "zip" in ctype or looks_like_zip(content):
        zip_path = data_dir / f"00981A_{today}.zip"
        zip_path.write_bytes(content)
        print(f"[OK] Saved ZIP to {zip_path}")

        # list zip contents
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                names = z.namelist()
                print("[INFO] ZIP contains:")
                for n in names:
                    print("  -", n)

                # try extract first .xlsx if any
                xlsx_candidates = [n for n in names if n.lower().endswith(".xlsx")]
                if xlsx_candidates:
                    pick = xlsx_candidates[0]
                    xlsx_bytes = z.read(pick)
                    xlsx_path = data_dir / f"00981A_{today}.xlsx"
                    xlsx_path.write_bytes(xlsx_bytes)
                    print(f"[OK] Extracted XLSX from ZIP -> {xlsx_path} (from {pick})")
                else:
                    print("[WARN] ZIP has no .xlsx inside. Kept ZIP for inspection; workflow will NOT fail.")
        except zipfile.BadZipFile:
            # sometimes "PK" but not a real zip, rare — keep raw bytes for debug
            raw_path = data_dir / f"00981A_{today}.bin"
            raw_path.write_bytes(content)
            print(f"[WARN] Content looked like ZIP but BadZipFile. Saved raw -> {raw_path}. workflow will NOT fail.")

    elif "spreadsheet" in ctype or "excel" in ctype or looks_like_xlsx(content):
        xlsx_path = data_dir / f"00981A_{today}.xlsx"
        xlsx_path.write_bytes(content)
        print(f"[OK] Saved XLSX to {xlsx_path}")

    elif looks_like_html(content) or "text/html" in ctype:
        html_path = data_dir / f"00981A_{today}.html"
        html_path.write_bytes(content)
        print(f"[WARN] Got HTML instead of file. Saved HTML -> {html_path} (workflow will NOT fail)")
    else:
        unknown_path = data_dir / f"00981A_{today}.bin"
        unknown_path.write_bytes(content)
        print(f"[WARN] Unknown content-type. Saved raw -> {unknown_path} (workflow will NOT fail)")


if __name__ == "__main__":
    download_and_save()
