import datetime
import pathlib
import requests
import zipfile
import io

BASE_URL = "https://www.ezmoney.com.tw"
FUND_CODE = "49YTW"  # 00981A 的 fundCode

INFO_URL = f"{BASE_URL}/ETF/Fund/Info?FundCode={FUND_CODE}"
EXPORT_URL = f"{BASE_URL}/ETF/Fund/AssetExcelNPOI?fundCode={FUND_CODE}"


def _is_zip_bytes(b: bytes) -> bool:
    # ZIP 檔頭通常是 PK\x03\x04 或 PK\x05\x06
    return len(b) >= 4 and b[0:2] == b"PK"


def download_and_save():
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0 Safari/537.36"
        )
    }

    # 1) 先打 Info 頁（比較穩）
    r1 = session.get(INFO_URL, headers=headers, timeout=30)
    r1.raise_for_status()
    print("[INFO] Opened Info page OK")

    # 2) 下載
    r2 = session.get(EXPORT_URL, headers=headers, timeout=60)
    r2.raise_for_status()
    ctype = (r2.headers.get("Content-Type") or "").lower()
    content = r2.content
    print(f"[INFO] Export Content-Type: {ctype}, bytes={len(content)}")

    data_dir = pathlib.Path("data") / "00981A"
    data_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.date.today().strftime("%Y%m%d")

    # 3) 如果是 zip：存 zip + 解出內部檔案（不分大小寫）
    if "zip" in ctype or _is_zip_bytes(content):
        zip_path = data_dir / f"00981A_{today}.zip"
        zip_path.write_bytes(content)
        print(f"[OK] Saved ZIP: {zip_path}")

        # 嘗試解 zip
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            names = zf.namelist()
            print("[DEBUG] ZIP contains:")
            for n in names:
                print(" -", n)

            # 找最可能的檔案：xlsx / xls / csv（不分大小寫）
            target = None
            for n in names:
                nl = n.lower()
                if nl.endswith((".xlsx", ".xls", ".csv")):
                    target = n
                    break

            if target is None:
                # 不要 fail，先把 zip 留著，後續你再決定怎麼處理
                print("[WARN] No .xlsx/.xls/.csv found inside ZIP. Kept ZIP for inspection.")
                # 另外把第一個檔案也解出來（如果有），方便你肉眼看
                if names:
                    first = names[0]
                    out = data_dir / f"00981A_{today}_firstfile.bin"
                    out.write_bytes(zf.read(first))
                    print(f"[WARN] Extracted first file as binary for debug: {out}")
                return

            # 解出 target
            extracted_bytes = zf.read(target)
            suffix = pathlib.Path(target).suffix.lower()  # .xlsx / .xls / .csv
            out_path = data_dir / f"00981A_{today}{suffix}"
            out_path.write_bytes(extracted_bytes)
            print(f"[OK] Extracted: {target} -> {out_path}")
            return

    # 4) 非 zip：當作 xlsx 直接存
    out_path = data_dir / f"00981A_{today}.xlsx"
    out_path.write_bytes(content)
    print(f"[OK] Saved XLSX: {out_path}")


if __name__ == "__main__":
    download_and_save()
