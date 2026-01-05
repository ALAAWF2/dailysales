import os
import json
import requests
import pandas as pd
import msal
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

# =========================
# CONFIG
# =========================
TIMEOUT = 120

# مهم: نكتب في جذر الريبو (جنب index.html)
OUTPUT_JSON = "../data.json"
INDEX_HTML = "../index.html"

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

    raise Exception(f"Auth failed: {result}")

# =========================
# FETCH (Today + Yesterday)
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

    query_url = (
        f"{BASE_URL}"
        f"?$filter=PaymentAmount ne 0 "
        f"and TransactionDate ge {start_str} "
        f"and TransactionDate lt {end_str}"
        f"&$select=OperatingUnitNumber,PaymentAmount,TransactionDate"
    )

    rows = []
    while query_url:
        r = requests.get(query_url, headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        rows.extend(data.get("value", []))
        query_url = data.get("@odata.nextLink")

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

    # نجبر GitHub Pages يعيد deploy
    if os.path.exists(INDEX_HTML):
        os.utime(INDEX_HTML, None)

    print("✅ data.json updated (Today & Yesterday)")

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
        return

    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])

    now_utc = datetime.now(timezone.utc)
    today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)

    df_today = df[df["TransactionDate"] >= today_start]
    df_yesterday = df[
        (df["TransactionDate"] >= yesterday_start) &
        (df["TransactionDate"] < today_start)
    ]

    mapping_df = load_mapping()

    export_json(
        process_group(df_today, mapping_df),
        process_group(df_yesterday, mapping_df)
    )

    # =========================
    # GIT PUSH
    # =========================
    push_to_github()

def push_to_github():
    github_token = os.getenv("GITHUB_TOKEN")
    repo_url = os.getenv("REPO_URL") # Optional: explicit override

    if not github_token:
        print("⚠️ GITHUB_TOKEN not found. Skipping git push.")
        return

    # Check if git repo
    if not os.path.exists(".git"):
        print("⚠️ Not a git repository. Skipping git push.")
        return

    try:
        import subprocess

        # Configure User
        subprocess.run(["git", "config", "user.email", "render-bot@example.com"], check=True)
        subprocess.run(["git", "config", "user.name", "Render Bot"], check=True)

        # Add file
        subprocess.run(["git", "add", "data.json"], check=True)

        # Commit
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if "data.json" not in status.stdout:
            print("✅ No changes in data.json to push.")
            return

        subprocess.run(["git", "commit", "-m", "Auto-update data.json [skip ci]"], check=True)

        # Push
        # If REPO_URL is not provided, try to get it from git config
        if not repo_url:
            try:
                result = subprocess.run(["git", "remote", "get-url", "origin"], capture_output=True, text=True, check=True)
                origin_url = result.stdout.strip()
                # Remove user/pass if any
                if "@" in origin_url:
                    origin_url = origin_url.split("@")[-1]
                repo_url = origin_url
            except Exception:
                print("⚠️ Could not detect origin URL. Please set REPO_URL env var.")
                return

        # Clean URL (remove https:// or http://)
        clean_url = repo_url.replace("https://", "").replace("http://", "")
        
        # Construct auth URL
        remote_with_token = f"https://{github_token}@{clean_url}"
        
        print(f"🚀 Pushing to {clean_url}...")
        subprocess.run(["git", "push", remote_with_token, "HEAD:main"], check=True)
        print("✅ Successfully pushed data.json to GitHub!")

    except Exception as e:
        print(f"❌ Git push failed: {e}")

if __name__ == "__main__":
    main()
