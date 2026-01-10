import datetime
import pathlib
import requests

BASE_URL = "https://www.ezmoney.com.tw"
FUND_CODE = "49YTW"  # 00981A 的 fundCode

# 先進 Info 頁讓網站設好 cookie（比較穩定）
INFO_URL = f"{BASE_URL}/ETF/Fund/Info?FundCode={FUND_CODE}"

# 真正的 XLSX 下載網址（從 getAssetXLSNPOI 推回來的）
EXPORT_URL = f"{BASE_URL}/ETF/Fund/AssetExcelNPOI?fundCode={FUND_CODE}"


def download_and_save():
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0 Safari/537.36"
        )
    }

    # 1️⃣ 先打 Info 頁，取得必要 cookie / session
    resp_info = session.get(INFO_URL, headers=headers, timeout=30)
    resp_info.raise_for_status()
    print("[INFO] 打開基金資訊頁成功")

    # 2️⃣ 直接打匯出 XLSX 的 API
    resp_xlsx = session.get(EXPORT_URL, headers=headers, timeout=60)
    resp_xlsx.raise_for_status()
    print(f"[INFO] 下載 API 回應 Content-Type: {resp_xlsx.headers.get('Content-Type')}")

    # 3️⃣ 存檔到 data 資料夾，以日期命名
    today = datetime.date.today().strftime("%Y%m%d")
    data_dir = pathlib.Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)

    filename = f"00981A_portfolio_{today}.xlsx"
    out_path = data_dir / filename
    out_path.write_bytes(resp_xlsx.content)

    print(f"[OK] Saved XLSX to {out_path}")


if __name__ == "__main__":
    download_and_save()
