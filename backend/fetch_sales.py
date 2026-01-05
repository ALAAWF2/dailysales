import os
import json
import requests
import pandas as pd
import msal
import subprocess
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

# =========================
# CONFIG
# =========================
TIMEOUT = 120

# نحفظ data.json في جذر الريبو (جنب index.html)
OUTPUT_JSON = "data.json"

# =========================
# LOAD ENV
# =========================
load_dotenv()

BASE_URL_ENV = os.getenv("D365_Url")
if BASE_URL_ENV:
    BASE_URL = f"{BASE_URL_ENV.rstrip('/')}/data/RetailTransactions"
else:
    BASE_URL = "https://orangepax.operations.eu.dynamics.com/data/RetailTransactions"

# =========================
# AUTH
# =========================
def get_access_token():
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    tenant_id = os.getenv("TENANT_ID")

    if not all([client_id, client_secret, tenant_id]):
        print("❌ Missing Azure AD credentials")
        return None

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    resource = "/".join(BASE_URL.split("/")[:3])
    scope = f"{resource}/.default"

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority,
    )

    result = app.acquire_token_for_client(scopes=[scope])

    if "access_token" in result:
        return result["access_token"]

    print("❌ Auth error:", result)
    raise Exception("Authentication failed")

# =========================
# FETCH (Today + Yesterday window)
# =========================
def fetch_sales_last_two_days(token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Prefer": "odata.maxpagesize=5000"
    }

    now_utc = datetime.now(timezone.utc)
    today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)
    tomorrow_start = today_start + timedelta(days=1)

    start_str = yesterday_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = tomorrow_start.strftime("%Y-%m-%dT%H:%M:%SZ")

    filter_query = (
        f"PaymentAmount ne 0 "
        f"and TransactionDate ge {start_str} "
        f"and TransactionDate lt {end_str}"
    )

    query_url = (
        f"{BASE_URL}"
        f"?$filter={filter_query}"
        f"&$select=OperatingUnitNumber,PaymentAmount,TransactionDate"
    )

    print(f"DEBUG: Fetching data from {start_str} to {end_str}")

    rows = []
    while query_url:
        r = requests.get(query_url, headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        rows.extend(data.get("value", []))
        query_url = data.get("@odata.nextLink")
        if query_url:
            print("DEBUG: Fetching next page...")

    return pd.DataFrame(rows)

# =========================
# MAPPING
# =========================
def load_mapping():
    if not os.path.exists("mapping.xlsx"):
        return None

    df = pd.read_excel("mapping.xlsx")
    df.columns = df.columns.str.lower().str.strip()

    store_col = next((c for c in df.columns if "store" in c and "number" in c), None)
    name_col = next((c for c in df.columns if "outlet" in c or "name" in c), None)

    if not store_col or not name_col:
        return None

    df[store_col] = df[store_col].astype(str).str.strip()
    return df[[store_col, name_col]].rename(
        columns={store_col: "store_id", name_col: "store_name"}
    )

# =========================
# TRANSFORM
# =========================
def process_group(df, mapping_df=None):
    if df.empty:
        return []

    df = df.copy()
    df["store"] = df["OperatingUnitNumber"].astype(str).str.strip()
    df["PaymentAmount"] = pd.to_numeric(df["PaymentAmount"], errors="coerce").fillna(0)

    grouped = df.groupby("store", as_index=False).agg(
        sales=("PaymentAmount", "sum")
    )

    if mapping_df is not None:
        grouped = grouped.merge(
            mapping_df, left_on="store", right_on="store_id", how="left"
        )
        grouped["outlet"] = grouped["store_name"].fillna(grouped["store"])
    else:
        grouped["outlet"] = grouped["store"]

    grouped = grouped.sort_values("sales", ascending=False)

    return [
        {"outlet": r["outlet"], "sales": int(r["sales"])}
        for _, r in grouped.iterrows()
    ]

# =========================
# EXPORT JSON
# =========================
def export_json(today_list, yesterday_list):
    payload = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "lastUpdate": datetime.now().strftime("%H:%M"),
        "today": today_list,
        "yesterday": yesterday_list
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print("✅ data.json updated (Today & Yesterday)")

# =========================
# GIT PUSH
# =========================
def git_commit_and_push():
    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPO")
    branch = os.getenv("GITHUB_BRANCH", "main")

    if not token or not repo:
        print("⚠️ GitHub credentials missing, skipping push")
        return

    repo_url = f"https://{token}@github.com/{repo}.git"

    try:
        subprocess.run(
            ["git", "config", "--global", "user.email", "cron@render.com"],
            check=True
        )
        subprocess.run(
            ["git", "config", "--global", "user.name", "Render Cron"],
            check=True
        )

        subprocess.run(["git", "add", "data.json"], check=True)
        subprocess.run(
            ["git", "commit", "-m", "Auto update sales data"],
            check=False
        )
        subprocess.run(["git", "push", repo_url, branch], check=True)

        print("✅ data.json committed & pushed to GitHub")

    except subprocess.CalledProcessError as e:
        print("❌ Git push failed:", e)

# =========================
# MAIN
# =========================
def main():
    print("🚀 Fetching Today & Yesterday sales...")

    token = get_access_token()
    if not token:
        return

    df = fetch_sales_last_two_days(token)
    if df.empty:
        export_json([], [])
        git_commit_and_push()
        return

    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])

    now_utc = datetime.now(timezone.utc)
    today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)

    try:
        df_today = df[df["TransactionDate"] >= pd.Timestamp(today_start)]
        df_yesterday = df[
            (df["TransactionDate"] >= pd.Timestamp(yesterday_start)) &
            (df["TransactionDate"] < pd.Timestamp(today_start))
        ]
    except:
        df["TransactionDate"] = df["TransactionDate"].dt.tz_localize(None)
        df_today = df[df["TransactionDate"] >= today_start.replace(tzinfo=None)]
        df_yesterday = df[
            (df["TransactionDate"] >= yesterday_start.replace(tzinfo=None)) &
            (df["TransactionDate"] < today_start.replace(tzinfo=None))
        ]

    mapping_df = load_mapping()

    list_today = process_group(df_today, mapping_df)
    list_yesterday = process_group(df_yesterday, mapping_df)

    export_json(list_today, list_yesterday)
    git_commit_and_push()

if __name__ == "__main__":
    main()



