import datetime
import pathlib
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://www.ezmoney.com.tw"
FUND_INFO_URL = "https://www.ezmoney.com.tw/ETF/Fund/Info?FundCode=49YTW"

def find_xlsx_url(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # 找到文字含「匯出」和「XLSX」的 a 標籤
    for a in soup.find_all("a"):
        text = (a.get_text() or "").strip()
        if "匯出" in text and "XLSX" in text.upper():
            href = a.get("href")
            if href:
                return urljoin(BASE_URL, href)
    raise RuntimeError("在頁面中找不到『匯出XLSX檔』的連結，請確認頁面有沒有改版。")

def download_and_save():
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0 Safari/537.36"
        )
    }

    resp = session.get(FUND_INFO_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    xlsx_url = find_xlsx_url(resp.text)

    xlsx_resp = session.get(xlsx_url, headers=headers, timeout=60)
    xlsx_resp.raise_for_status()

    today = datetime.date.today().strftime("%Y%m%d")
    data_dir = pathlib.Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    filename = f"00981A_portfolio_{today}.xlsx"
    out_path = data_dir / filename

    out_path.write_bytes(xlsx_resp.content)
    print(f"Saved XLSX to {out_path}")

if __name__ == "__main__":
    download_and_save()
