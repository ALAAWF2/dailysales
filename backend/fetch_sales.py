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

# Determine the absolute path to the backend directory
BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
# Construct paths relative to the backend directory
# intended: backend/../data.json -> root/data.json
OUTPUT_JSON = os.path.normpath(os.path.join(BACKEND_DIR, "..", "data.json"))
INDEX_HTML = os.path.normpath(os.path.join(BACKEND_DIR, "..", "index.html"))

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
# =========================
# FETCH MTD (Month to Date)
# =========================
def fetch_sales_mtd_range(token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Prefer": "odata.maxpagesize=5000"
    }

    now_utc = datetime.now(timezone.utc)
    # Start of current month
    month_start = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # End of "today" (so we get everything up to now)
    tomorrow_start = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    start_str = month_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = tomorrow_start.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"📅 Fetching MTD data from {start_str} to {end_str}...")

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
    # Use absolute path for mapping file
    mapping_path = os.path.join(BACKEND_DIR, "mapping.xlsx")
    
    if not os.path.exists(mapping_path):
        print(f"⚠️ Mapping file not found at: {mapping_path}")
        return None

    df = pd.read_excel(mapping_path)
    # Normalize headers
    df.columns = df.columns.str.lower().str.strip()

    store_col = next((c for c in df.columns if "store" in c and "number" in c), None)
    name_col = next((c for c in df.columns if "outlet" in c or "name" in c), None)
    
    # New columns
    target_col = next((c for c in df.columns if "target" in c), None)
    city_col = next((c for c in df.columns if "city" in c), None)
    area_col = next((c for c in df.columns if "area" in c), None)

    if not store_col or not name_col:
        print("❌ Critical columns (store, name) missing in mapping.")
        return None

    # Rename for consistency
    rename_map = {store_col: "store_id", name_col: "store_name"}
    
    if target_col: rename_map[target_col] = "target"
    if city_col: rename_map[city_col] = "city"
    if area_col: rename_map[area_col] = "area"

    df[store_col] = df[store_col].astype(str).str.strip()
    
    final_cols = ["store_id", "store_name"]
    if target_col: final_cols.append("target")
    if city_col: final_cols.append("city")
    if area_col: final_cols.append("area")

    return df[list(rename_map.keys())].rename(columns=rename_map)

# =========================
# TRANSFORM
# =========================
def process_group(df, mapping_df=None, is_mtd=False):
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
        
        # Add metadata for filters
        if "city" in grouped.columns:
            grouped["city"] = grouped["city"].fillna("Unknown")
        if "area" in grouped.columns:
            grouped["area"] = grouped["area"].fillna("Unknown")
        if "target" in grouped.columns:
            grouped["target"] = pd.to_numeric(grouped["target"], errors="coerce").fillna(0)
    else:
        grouped["outlet"] = grouped["store"]
        grouped["city"] = "Unknown"
        grouped["area"] = "Unknown"
        grouped["target"] = 0

    grouped = grouped.sort_values("sales", ascending=False)

    results = []
    for _, r in grouped.iterrows():
        item = {
            "outlet": r["outlet"],
            "sales": int(r["sales"]),
            "city": r.get("city", "Unknown"),
            "area": r.get("area", "Unknown")
        }
        if is_mtd:
             item["target"] = int(r.get("target", 0))
        
        results.append(item)
        
    return results

# =========================
# EXPORT JSON
# =========================
def export_json(today_list, yesterday_list, mtd_list, cities, areas):
    # Adjust time to UTC+3 (or user's local time)
    now_local = datetime.now(timezone.utc) + timedelta(hours=3)

    payload = {
        "date": now_local.strftime("%Y-%m-%d"),
        "lastUpdate": now_local.strftime("%I:%M %p"),
        "today": today_list,
        "yesterday": yesterday_list,
        "mtd": mtd_list,
        "metadata": {
            "cities": sorted(list(cities)),
            "areas": sorted(list(areas))
        }
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # Force GitHub Pages update simulation (local timestamp update)
    if os.path.exists(INDEX_HTML):
        os.utime(INDEX_HTML, None)

    print("✅ data.json updated (Today, Yesterday & MTD)")

# =========================
# MAIN
# =========================
def main():
    print("🚀 Fetching Sales Data...")

    token = get_access_token()
    if not token:
        return

    # 1. Fetch MTD Range (includes today and yesterday)
    # Optimization: We can just fetch MTD once and filter in memory for Today/Yesterday
    # to avoid multiple API calls if volume permits. 
    # But sticking to separate calls or splitting the huge dataframe?
    # Actually, fetching MTD (1st - Now) covers everything. Let's do that to be efficient.
    
    df_all = fetch_sales_mtd_range(token)
    
    if df_all.empty:
        export_json([], [], [], [], [])
        return

    df_all["TransactionDate"] = pd.to_datetime(df_all["TransactionDate"])
    
    # Time Ranges
    now_utc = datetime.now(timezone.utc)
    today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)

    # Filter
    df_today = df_all[df_all["TransactionDate"] >= today_start]
    df_yesterday = df_all[
        (df_all["TransactionDate"] >= yesterday_start) &
        (df_all["TransactionDate"] < today_start)
    ]
    # df_mtd is just df_all (assuming we fetched from 1st of month)

    mapping_df = load_mapping()

    today_data = process_group(df_today, mapping_df, is_mtd=False)
    yesterday_data = process_group(df_yesterday, mapping_df, is_mtd=False)
    mtd_data = process_group(df_all, mapping_df, is_mtd=True)

    # Extract unique cities and areas for frontend filters
    cities = set()
    areas = set()
    if mapping_df is not None:
        if "city" in mapping_df.columns:
            cities = set(mapping_df["city"].dropna().unique())
        if "area" in mapping_df.columns:
            areas = set(mapping_df["area"].dropna().unique())

    export_json(today_data, yesterday_data, mtd_data, cities, areas)

    # =========================
    # GIT PUSH
    # =========================
    # print("🔒 Git push disabled for verification.")
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

        # Ensure we have the URL
        if not repo_url:
            try:
                result = subprocess.run(["git", "remote", "get-url", "origin"], capture_output=True, text=True, check=True)
                origin_url = result.stdout.strip()
                if "@" in origin_url:
                    origin_url = origin_url.split("@")[-1]
                repo_url = origin_url
            except Exception:
                print("⚠️ Could not detect origin URL. Please set REPO_URL env var.")
                return

        clean_url = repo_url.replace("https://", "").replace("http://", "")
        if "@" in clean_url:
             clean_url = clean_url.split("@")[-1]
             
        remote_with_token = f"https://{github_token}@{clean_url}"

        # Configure remote URL safely
        existing_remotes = subprocess.run(["git", "remote"], capture_output=True, text=True).stdout.splitlines()
        
        if "origin" in existing_remotes:
            # Update existing remote
            subprocess.run(["git", "remote", "set-url", "origin", remote_with_token], check=True)
        else:
            # Add new remote if missing
            subprocess.run(["git", "remote", "add", "origin", remote_with_token], check=True)

        print("🔄 Syncing with remote (Robust Mode)...")
        # 1. Fetch latest state
        subprocess.run(["git", "fetch", "origin", "main"], check=True)

        # 2. Read the FRESH data.json we just generated into memory
        #    (So we can restore it after resetting git state)
        with open(OUTPUT_JSON, 'r', encoding='utf-8') as f:
            new_data_content = f.read()

        # 3. Force switch to main branch and reset to match remote exactly
        #    (This fixes 'detached HEAD' and 'divergent branch' issues)
        subprocess.run(["git", "checkout", "-B", "main", "origin/main"], check=True)

        # 4. Write data.json back to disk
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            f.write(new_data_content)
        
        # Force timestamp update for GitHub Pages trigger if needed
        if os.path.exists(INDEX_HTML):
             os.utime(INDEX_HTML, None)

        # 5. Add, Commit, Push
        subprocess.run(["git", "add", OUTPUT_JSON], check=True)

        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if "data.json" not in status.stdout:
            print("✅ No changes in data.json to push.")
            return

        subprocess.run(["git", "commit", "-m", "Auto-update data.json [skip ci]"], check=True)

        print(f"🚀 Pushing to {clean_url}...")
        subprocess.run(["git", "push", "origin", "main"], check=True)
        print("✅ Successfully pushed data.json to GitHub!")

    except Exception as e:
        print(f"❌ Git push failed: {e}")

if __name__ == "__main__":
    main()
