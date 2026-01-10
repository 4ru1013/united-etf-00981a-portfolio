import datetime as dt
import pathlib
import zipfile
from io import BytesIO

import pandas as pd
import requests

BASE_URL = "https://www.ezmoney.com.tw"
FUND_CODE = "49YTW"  # 00981A 的 fundCode

INFO_URL = f"{BASE_URL}/ETF/Fund/Info?FundCode={FUND_CODE}"
EXPORT_URL = f"{BASE_URL}/ETF/Fund/AssetExcelNPOI?fundCode={FUND_CODE}"

DATA_DIR = pathlib.Path("data")
RAW_DIR = DATA_DIR / "raw"
PARSED_DIR = DATA_DIR / "parsed"
DIFF_DIR = DATA_DIR / "diff"
LATEST_DIR = DATA_DIR / "latest"


def is_zip_bytes(b: bytes) -> bool:
    # ZIP magic: PK\x03\x04 or PK\x05\x06 (empty) or PK\x07\x08 (spanned)
    return len(b) >= 4 and b[:2] == b"PK"


def is_xlsx_bytes(b: bytes) -> bool:
    # xlsx 本質也是 zip，但內容會包含 xl/ 目錄
    if not is_zip_bytes(b):
        return False
    try:
        with zipfile.ZipFile(BytesIO(b)) as zf:
            names = set(zf.namelist())
            return any(n.startswith("xl/") for n in names)
    except zipfile.BadZipFile:
        return False


def extract_xlsx_from_zip(zip_bytes: bytes) -> bytes:
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        # 找 zip 裡面第一個 .xlsx
        xlsx_candidates = [n for n in zf.namelist() if n.lower().endswith(".xlsx")]
        if not xlsx_candidates:
            # 有些 zip 不是 xlsx，而是其他格式；先把清單丟出來方便 debug
            raise RuntimeError(f"ZIP 裡沒有 .xlsx，內容清單：{zf.namelist()[:30]}")
        with zf.open(xlsx_candidates[0]) as f:
            return f.read()


def safe_mkdirs():
    for d in [DATA_DIR, RAW_DIR, PARSED_DIR, DIFF_DIR, LATEST_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def download_bytes() -> bytes:
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0 Safari/537.36"
        )
    }

    resp_info = session.get(INFO_URL, headers=headers, timeout=30)
    resp_info.raise_for_status()
    print("[INFO] 打開基金資訊頁成功")

    resp = session.get(EXPORT_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    ct = resp.headers.get("Content-Type", "")
    print(f"[INFO] 下載 API 回應 Content-Type: {ct}")
    return resp.content


def normalize_to_xlsx_bytes(content: bytes) -> bytes:
    """
    可能回：
    - 直接 xlsx（其實也是 zip，但含 xl/）
    - zip 包 xlsx
    - HTML（被擋/驗證頁）
    """
    if is_xlsx_bytes(content):
        return content

    if is_zip_bytes(content):
        # 是 zip 但不是標準 xlsx（可能 zip 包 xlsx）
        return extract_xlsx_from_zip(content)

    # 不是 zip => 可能是 html / 文字
    head = content[:200].decode("utf-8", errors="ignore")
    raise RuntimeError(
        "下載內容不是 xlsx/zip。可能被擋或回傳 HTML。\n"
        f"前 200 bytes:\n{head}"
    )


def read_xlsx_to_df(xlsx_bytes: bytes) -> pd.DataFrame:
    # 先把 xlsx 存成 BytesIO 讀進來
    bio = BytesIO(xlsx_bytes)

    # 這邊不假設 sheet 名稱，先讀第一個 sheet
    xls = pd.ExcelFile(bio, engine="openpyxl")
    df = pd.read_excel(xls, sheet_name=xls.sheet_names[0], engine="openpyxl")

    # 清理欄名
    df.columns = [str(c).strip() for c in df.columns]
    return df


def find_trade_date(df: pd.DataFrame) -> str:
    """
    優先：從檔案內找「資料日期」/「日期」等字樣。
    若找不到，就用今天日期。
    """
    # 盡量從表格裡找包含日期的欄位/值
    candidates = []
    for col in df.columns:
        if any(k in col for k in ["日期", "資料日期", "Data", "Date"]):
            candidates.append(col)

    # 若有日期欄，試著取第一個非空
    for col in candidates:
        s = df[col].dropna()
        if not s.empty:
            v = s.iloc[0]
            try:
                d = pd.to_datetime(v).date()
                return d.strftime("%Y%m%d")
            except Exception:
                pass

    # fallback：今天
    return dt.date.today().strftime("%Y%m%d")


def standardize_holdings(df: pd.DataFrame) -> pd.DataFrame:
    """
    把 ezmoney 匯出的表整理成固定欄位：
    stock_id, stock_name, shares
    """
    # 常見欄位可能是：股票代號 / 股票名稱 / 持股股數 或 單位數 等
    # 這裡用「包含字」的方式去猜
    colmap = {}

    def pick_col(keys):
        for k in keys:
            for c in df.columns:
                if k in str(c):
                    return c
        return None

    code_col = pick_col(["股票代號", "證券代號", "代號", "Stock", "Code"])
    name_col = pick_col(["股票名稱", "證券名稱", "名稱", "Name"])
    shares_col = pick_col(["股數", "持股股數", "數量", "單位", "持股數", "Shares", "Units"])

    if not code_col or not shares_col:
        raise RuntimeError(
            "找不到必要欄位（代號/股數）。\n"
            f"目前欄位：{list(df.columns)}"
        )

    out = df[[code_col] + ([name_col] if name_col else []) + [shares_col]].copy()
    out.columns = ["stock_id"] + (["stock_name"] if name_col else []) + ["shares"]

    # 清理
    out["stock_id"] = out["stock_id"].astype(str).str.strip()
    if "stock_name" in out.columns:
        out["stock_name"] = out["stock_name"].astype(str).str.strip()

    # shares 轉數字（去逗號）
    out["shares"] = (
        out["shares"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace(" ", "", regex=False)
    )
    out["shares"] = pd.to_numeric(out["shares"], errors="coerce").fillna(0).astype("int64")

    # 去掉代號空的列
    out = out[out["stock_id"].str.len() > 0].copy()

    # 同代號合併（保險）
    grp_cols = ["stock_id"] + (["stock_name"] if "stock_name" in out.columns else [])
    out = out.groupby(grp_cols, as_index=False)["shares"].sum()

    return out.sort_values(["shares"], ascending=False).reset_index(drop=True)


def find_previous_parsed_csv(today_yyyymmdd: str) -> pathlib.Path | None:
    # 找 parsed 內「日期 < today」的最新一個
    csvs = sorted(PARSED_DIR.glob("00981A_holdings_*.csv"))
    prev = None
    for p in csvs:
        # 00981A_holdings_YYYYMMDD.csv
        stem = p.stem
        ymd = stem.split("_")[-1]
        if ymd.isdigit() and ymd < today_yyyymmdd:
            prev = p
    return prev


def diff_holdings(today_df: pd.DataFrame, prev_df: pd.DataFrame) -> pd.DataFrame:
    # 以 stock_id 為主鍵
    t = today_df.copy()
    p = prev_df.copy()

    if "stock_name" not in t.columns:
        t["stock_name"] = ""
    if "stock_name" not in p.columns:
        p["stock_name"] = ""

    merged = t.merge(
        p[["stock_id", "stock_name", "shares"]].rename(columns={"shares": "shares_prev"}),
        on="stock_id",
        how="outer",
        suffixes=("", "_prev"),
    )

    # stock_name：優先今天，沒有就用昨天
    merged["stock_name"] = merged["stock_name"].replace("nan", "")
    merged["stock_name_prev"] = merged.get("stock_name_prev", "").astype(str)
    merged["stock_name"] = merged["stock_name"].where(merged["stock_name"].astype(str).str.len() > 0, merged["stock_name_prev"])

    merged["shares"] = merged["shares"].fillna(0).astype("int64")
    merged["shares_prev"] = merged["shares_prev"].fillna(0).astype("int64")
    merged["delta_shares"] = merged["shares"] - merged["shares_prev"]

    def classify(row):
        if row["shares_prev"] == 0 and row["shares"] > 0:
            return "新增"
        if row["shares_prev"] > 0 and row["shares"] == 0:
            return "出清"
        if row["delta_shares"] > 0:
            return "加碼"
        if row["delta_shares"] < 0:
            return "減碼"
        return "持平"

    merged["action"] = merged.apply(classify, axis=1)

    # 排序：先看新增/出清，再看變動幅度
    order = {"新增": 0, "出清": 1, "加碼": 2, "減碼": 3, "持平": 4}
    merged["action_rank"] = merged["action"].map(order).fillna(99).astype(int)

    merged = merged.sort_values(
        ["action_rank", "delta_shares"],
        ascending=[True, False],
    ).drop(columns=["action_rank", "stock_name_prev"], errors="ignore")

    return merged.reset_index(drop=True)


def main():
    safe_mkdirs()

    raw = download_bytes()

    # 保存原始檔（方便 debug）
    today = dt.date.today().strftime("%Y%m%d")
    raw_path = RAW_DIR / f"00981A_raw_{today}.bin"
    raw_path.write_bytes(raw)

    xlsx_bytes = normalize_to_xlsx_bytes(raw)

    # 保存 xlsx
    xlsx_path = RAW_DIR / f"00981A_portfolio_{today}.xlsx"
    xlsx_path.write_bytes(xlsx_bytes)
    print(f"[OK] Saved XLSX to {xlsx_path}")

    df0 = read_xlsx_to_df(xlsx_bytes)
    data_date = find_trade_date(df0)  # 優先用檔內日期
    print(f"[INFO] data_date = {data_date}")

    holdings = standardize_holdings(df0)

    # 輸出 parsed
    parsed_path = PARSED_DIR / f"00981A_holdings_{data_date}.csv"
    holdings.to_csv(parsed_path, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved parsed holdings to {parsed_path}")

    # 更新 latest
    latest_path = LATEST_DIR / "00981A_holdings_latest.csv"
    holdings.to_csv(latest_path, index=False, encoding="utf-8-sig")

    # diff（如果有前一天）
    prev_path = find_previous_parsed_csv(data_date)
    if prev_path is None:
        print("[INFO] 找不到前一個 parsed CSV，跳過 diff")
        return

    prev_df = pd.read_csv(prev_path)
    diff_df = diff_holdings(holdings, prev_df)

    diff_path = DIFF_DIR / f"00981A_diff_{prev_path.stem.split('_')[-1]}_to_{data_date}.csv"
    diff_df.to_csv(diff_path, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved diff to {diff_path}")

    # 也順便產生一份摘要 markdown
    md_path = DIFF_DIR / f"00981A_diff_{prev_path.stem.split('_')[-1]}_to_{data_date}.md"
    def top_lines(action, n=15):
        sub = diff_df[diff_df["action"] == action].copy()
        if sub.empty:
            return ["- （無）"]
        # 只列出前 n
        lines = []
        for _, r in sub.head(n).iterrows():
            lines.append(f"- {r['stock_id']} {str(r.get('stock_name',''))}：{r['shares_prev']} → {r['shares']}（{r['delta_shares']:+,}）")
        return lines

    md = []
    md.append(f"# 00981A 持股變動 {prev_path.stem.split('_')[-1]} → {data_date}\n")
    md.append("## 新增\n" + "\n".join(top_lines("新增")))
    md.append("\n## 出清\n" + "\n".join(top_lines("出清")))
    md.append("\n## 加碼（前 15）\n" + "\n".join(top_lines("加碼")))
    md.append("\n## 減碼（前 15）\n" + "\n".join(top_lines("減碼")))
    md_path.write_text("\n".join(md), encoding="utf-8")
    print(f"[OK] Saved diff summary to {md_path}")


if __name__ == "__main__":
    main()
