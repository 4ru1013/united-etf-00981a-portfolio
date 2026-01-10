import datetime
import pathlib
import requests

BASE_URL = "https://www.ezmoney.com.tw"
FUND_CODE = "49YTW"  # 00981A
INFO_URL = f"{BASE_URL}/ETF/Fund/Info?FundCode={FUND_CODE}"
EXPORT_URL = f"{BASE_URL}/ETF/Fund/AssetExcelNPOI?fundCode={FUND_CODE}"

def download_and_save_zip():
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0 Safari/537.36"
        )
    }

    # 先打 Info 頁建立 session/cookie
    resp_info = session.get(INFO_URL, headers=headers, timeout=30)
    resp_info.raise_for_status()

    resp = session.get(EXPORT_URL, headers=headers, timeout=60)
    resp.raise_for_status()

    today = datetime.date.today().strftime("%Y%m%d")
    out_dir = pathlib.Path("data") / "00981A"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 不管 Content-Type，原封不動存成 zip（或 raw）
    out_path = out_dir / f"00981A_{today}.zip"
    out_path.write_bytes(resp.content)

    print(f"[OK] Saved to {out_path}")
    print(f"[INFO] Content-Type: {resp.headers.get('Content-Type')} size={len(resp.content)}")

if __name__ == "__main__":
    download_and_save_zip()