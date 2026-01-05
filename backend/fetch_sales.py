
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
OUTPUT_JSON = "data.json"

# Load env here to ensure they are available
load_dotenv()
D365_URL = os.getenv('D365_Url')
if not D365_URL:
    # Fallback or error if not set, but script uses it for base URL construction logic if needed
    # The user's script hardcoded the base URL in their snippet, but let's try to be dynamic if possible
    # or stick to their logic which had a hardcoded base url.
    # User's snippet: BASE_URL = "https://orangepax.operations.eu.dynamics.com/data/RetailTransactions"
    # I will use the env var if available, else their hardcoded one.
    pass

# We will use the env var for the base URL to match the .env file instruction I gave earlier
# But if the user provided script has a specific URL, I should probably respect it or make it configurable.
# I will use the one from .env if possible, to keep it clean.
# Actually, the user's script snippet had:
# BASE_URL = "https://orangepax.operations.eu.dynamics.com/data/RetailTransactions"
# I should probably update this to use the D365_Url from .env + /data/RetailTransactions

BASE_URL_ENV = os.getenv('D365_Url')
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
        print("❌ Error: Missing credentials in .env file")
        return None

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    
    # Scope should likely be the resource URL + .default
    # Extract resource from BASE_URL
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
    else:
        print(f"❌ Auth Error: {result.get('error')}")
        print(f"Description: {result.get('error_description')}")
        raise Exception("Authentication failed")

# =========================
# FETCH
# =========================
# =========================
# FETCH
# =========================
def fetch_month_sales(token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Prefer": "odata.maxpagesize=5000"
    }

    now_utc = datetime.now(timezone.utc)
    # Start of current month
    start_of_month = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_str = start_of_month.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # End of today (effectively functionality is "up to now", but for OData we use lt Tomorrow)
    tomorrow = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_str = tomorrow.strftime("%Y-%m-%dT%H:%M:%SZ")

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

    print(f"DEBUG: Requesting Month Data from {start_str}...")
    
    rows = []
    while query_url:
        try:
            r = requests.get(query_url, headers=headers, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if "value" in data:
                rows.extend(data["value"])
            query_url = data.get("@odata.nextLink")
            if query_url: print("DEBUG: Fetching next page...")
        except Exception as e:
            print(f"❌ Error: {e}")
            break # Stop on error but process what we have

    return pd.DataFrame(rows)

# =========================
# TRANSFORM
# =========================
def process_group(df, mapping_df=None):
    if df.empty:
        return []

    # Clean
    if "OperatingUnitNumber" in df.columns:
        df["store"] = df["OperatingUnitNumber"].astype(str).str.strip()
    else:
        df["store"] = "Unknown"

    df["PaymentAmount"] = pd.to_numeric(df["PaymentAmount"], errors='coerce').fillna(0)

    # Aggregate
    grouped = df.groupby("store", as_index=False).agg(sales=("PaymentAmount", "sum"))

    # Map Names
    if mapping_df is not None:
        try:
            # Assumes mapping_df is already prepared with string columns 'store_id' and 'name'
            # We redo the logic inside load_mapping for safety or pass it in.
            # Simplified: Merge directly
            merged = pd.merge(grouped, mapping_df, left_on="store", right_on="store_id", how="left")
            merged["outlet"] = merged["store_name"].fillna(merged["store"])
            final = merged[["outlet", "sales"]]
        except Exception as e:
             print(f"Mapping apply error: {e}")
             final = grouped.rename(columns={"store": "outlet"})
    else:
        final = grouped.rename(columns={"store": "outlet"})

    # Sort and Dict
    final = final.sort_values("sales", ascending=False)
    
    return [
        {"outlet": r["outlet"], "sales": int(r["sales"])} 
        for _, r in final.iterrows()
    ]

def load_mapping():
    if not os.path.exists("mapping.xlsx"):
        return None
    
    try:
        print("Loading mapping...")
        df = pd.read_excel("mapping.xlsx")
        df.columns = df.columns.str.lower().str.strip()
        
        # Identify columns
        store_col = next((c for c in df.columns if 'store' in c and 'number' in c), 
                        next((c for c in df.columns if 'code' in c), None))
        name_col = next((c for c in df.columns if 'outlet' in c or 'name' in c), None)

        if store_col and name_col:
            df[store_col] = df[store_col].astype(str).str.strip()
            return df[[store_col, name_col]].rename(columns={store_col: "store_id", name_col: "store_name"})
    except:
        pass
    return None

# =========================
# EXPORT JSON
# =========================
def export_json(today_list, month_list):
    payload = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "lastUpdate": datetime.now().strftime("%H:%M"),
        "today": today_list,
        "month": month_list
    }

    try:
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print("✅ data.json updated (Month & Today modes)")
    except Exception as e:
        print(f"❌ Error writing JSON: {e}")

# =========================
# MAIN
# =========================
def main():
    print("🚀 Fetching Month-To-Date sales...")
    try:
        token = get_access_token()
        if not token: return
            
        df_all = fetch_month_sales(token)
        if df_all.empty:
            print("No data found.")
            export_json([], [])
            return

        # Prepare dates for splitting
        df_all["TransactionDate"] = pd.to_datetime(df_all["TransactionDate"])
        
        # Today's Date (Local logic or UTC? Source is UTC)
        # We will assume 'Today' is based on the system running the script (Local)
        # But data is UTC. 
        # Better approach: Convert Data to Local or keep UTC matching. 
        # User said "Today filter".
        # Let's use strict UTC "Today" from 00:00Z to 23:59Z for consistency with D365 usually.
        # OR use the same logic as previous script: Today Start UTC.
        
        now_utc = datetime.now(timezone.utc)
        today_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Filter Today
        # Try/Catch for timezone awareness mismatch
        try:
             df_today = df_all[df_all["TransactionDate"] >= pd.Timestamp(today_start_utc)]
        except:
             # Fallback if tz-naive
             df_all["TransactionDate"] = df_all["TransactionDate"].dt.tz_localize(None) 
             today_start_naive = today_start_utc.replace(tzinfo=None)
             df_today = df_all[df_all["TransactionDate"] >= today_start_naive]

        # Load Map
        map_df = load_mapping()

        # Process Both
        list_today = process_group(df_today, map_df)
        list_month = process_group(df_all, map_df)

        export_json(list_today, list_month)
        
    except Exception as e:
        print(f"❌ Critical Failure: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
