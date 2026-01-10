import datetime
import pathlib
import requests
import zipfile
from io import BytesIO

BASE_URL = "https://www.ezmoney.com.tw"
FUND_CODE = "49YTW"  # 00981A 的 fundCode
INFO_URL = f"{BASE_URL}/ETF/Fund/Info?FundCode={FUND_CODE}"
EXPORT_URL = f"{BASE_URL}/ETF/Fund/AssetExcelNPOI?fundCode={FUND_CODE}"

def is_zip_bytes(b: bytes) -> bool:
    return len(b) >= 4 and b[:2] == b"PK"

def download_and_save():
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0 Safari/537.36"
        )
    }

    # 1) 先打 Info 頁，讓 cookie/session 就位
    resp_info = session.get(INFO_URL, headers=headers, timeout=30)
    resp_info.raise_for_status()
    print("[INFO] Open Info page OK")

    # 2) 下載
    resp = session.get(EXPORT_URL, headers=headers, timeout=60)
    resp.raise_for_status()

    ctype = (resp.headers.get("Content-Type") or "").lower()
    print(f"[INFO] Content-Type: {ctype}")
    content = resp.content
    print(f"[INFO] Payload size: {len(content)} bytes")

    today = datetime.date.today().strftime("%Y%m%d")
    base_dir = pathlib.Path("data") / "00981A"
    base_dir.mkdir(parents=True, exist_ok=True)

    # 3) 先把 raw 回應存起來（debug 超有用）
    raw_path = base_dir / f"raw_{today}.bin"
    raw_path.write_bytes(content)
    print(f"[DEBUG] Saved raw payload to {raw_path}")

    # 4) 若回應其實是 HTML（被擋/錯誤頁），存成 html 方便看
    if b"<html" in content[:2000].lower() or "text/html" in ctype:
        html_path = base_dir / f"blocked_{today}.html"
        html_path.write_bytes(content)
        raise RuntimeError(f"Got HTML instead of file. Saved to {html_path}")

    # 5) 正常情況：直接是 xlsx
    if "xlsx" in ctype or content[:4] == b"PK\x03\x04":
        # 但這也可能是 zip，所以後面還是會檢查
        pass

    # 6) 若是 zip：列出內容、抓出可能的檔案
    if is_zip_bytes(content):
        zip_path = base_dir / f"00981A_{today}.zip"
        zip_path.write_bytes(content)
        print(f"[INFO] ZIP saved to {zip_path}")

        with zipfile.ZipFile(BytesIO(content)) as z:
            names = z.namelist()
            print("[INFO] ZIP content list:")
            for n in names:
                print("  -", n)

            # 允許的副檔名：xlsx / xls / csv
            candidates = [
                n for n in names
                if n.lower().endswith((".xlsx", ".xls", ".csv"))
            ]

            if candidates:
                # 選第一個最像的（你也可以改成排序挑最短/最深）
                target = candidates[0]
                out_path = base_dir / target.split("/")[-1]
                out_path.write_bytes(z.read(target))
                print(f"[OK] Extracted file: {target} -> {out_path}")
                return

            # 如果沒有 xlsx/xls/csv，就把全部解出來方便你看
            extract_dir = base_dir / f"zip_extract_{today}"
            extract_dir.mkdir(parents=True, exist_ok=True)
            z.extractall(extract_dir)
            raise RuntimeError(
                f"ZIP returned but no xlsx/xls/csv inside. "
                f"Extracted all to {extract_dir}"
            )

    # 7) 若不是 zip，也不是 html，那就直接當 xlsx 存
    out_xlsx = base_dir / f"00981A_{today}.xlsx"
    out_xlsx.write_bytes(content)
    print(f"[OK] Saved as XLSX to {out_xlsx}")

if __name__ == "__main__":
    download_and_save()