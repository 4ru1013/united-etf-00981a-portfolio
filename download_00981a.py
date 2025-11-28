import datetime
import pathlib
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://www.ezmoney.com.tw"
FUND_INFO_URL = "https://www.ezmoney.com.tw/ETF/Fund/Info?FundCode=49YTW"


def find_xlsx_url(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    candidates = []

    for a in soup.find_all("a"):
        text = (a.get_text() or "").strip()
        href = a.get("href") or ""

        # 只要文字或連結裡有 XLS 就先視為候選
        if "XLS" in text.upper() or "XLS" in href.upper():
            full_url = urljoin(BASE_URL, href) if href else ""
            print(f"[CANDIDATE] text='{text}', href='{href}', full='{full_url}'")
            if href:
                candidates.append(full_url)

    if len(candidates) == 1:
        print("[INFO] 使用唯一候選連結")
        return candidates[0]
    elif len(candidates) > 1:
        print("[INFO] 找到多個候選連結，先使用第一個")
        return candidates[0]

    # 如果完全找不到候選，再用原本的「匯出XLSX檔」文字搜尋一次
    for a in soup.find_all(["a", "button"]):
        text = (a.get_text() or "").strip()
        if "匯出XLSX" in text or "匯出 XLSX" in text.replace(" ", ""):
            href = a.get("href")
            if href:
                full_url = urljoin(BASE_URL, href)
                print(f"[FALLBACK] 依照文字匹配找到連結: {full_url}")
                return full_url

    raise RuntimeError("在頁面中找不到任何疑似 XLSX 的下載連結，可能是網站改版。")


def download_and_save():
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0 Safari/537.36"
        )
    }

    try:
        resp = session.get(FUND_INFO_URL, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"[ERROR] 無法開啟基金投資組合頁面：{e}")
        sys.exit(1)

    html = resp.text

    # 存 HTML 快照，方便之後 debug
    today = datetime.date.today().strftime("%Y%m%d")
    snapshot_dir = pathlib.Path("data/html_snapshot")
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / f"page_{today}.html"
    snapshot_path.write_text(html, encoding="utf-8")
    print(f"[INFO] 已將頁面 HTML 存在 {snapshot_path}")

    try:
        xlsx_url = find_xlsx_url(html)
    except Exception as e:
        print(f"[ERROR] 找不到 XLSX 下載連結：{e}")
        print("[INFO] 目前先只保存 HTML，之後再分析下載連結。")
        return  # 直接結束函式，不再往下抓 XLSX

    print(f"[INFO] 下載連結：{xlsx_url}")
    
    try:
        xlsx_resp = session.get(xlsx_url, headers=headers, timeout=60)
        xlsx_resp.raise_for_status()
    except Exception as e:
        print(f"[ERROR] 下載 XLSX 失敗：{e}")
        sys.exit(1)

    data_dir = pathlib.Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    filename = f"00981A_portfolio_{today}.xlsx"
    out_path = data_dir / filename

    out_path.write_bytes(xlsx_resp.content)
    print(f"[OK] Saved XLSX to {out_path}")


if __name__ == "__main__":
    download_and_save()
