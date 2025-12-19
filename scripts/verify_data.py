import pandas as pd
import mysql.connector

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "abubakar123",  
    "database": "BlackDW"
}

PATHS = {
    "stream": "D:/semster6/data_warehouse/PROJECT/transactional_data.csv",
    "product": "D:/semster6/data_warehouse/PROJECT/product_master_data.csv"
}

def verify_data():
    print("="*60)
    print("🔎 STARTING DATA AUDIT (SOURCE CSV vs. TARGET DB)")
    print("="*60)

    # ---------------------------------------------------------
    # 1. CALCULATE EXPECTED RESULTS (FROM CSV)
    # ---------------------------------------------------------
    print("[1/3] Loading and processing CSV files in Python...")
    
    # Load Data
    stream_df = pd.read_csv(PATHS["stream"], dtype=str)
    product_df = pd.read_csv(PATHS["product"], dtype=str)

    # Clean Stream: Remove Duplicates (Simulating the ETL logic)
    # The ETL ignores a record if OrderID + ProductID has been seen before
    initial_count = len(stream_df)
    stream_df = stream_df.drop_duplicates(subset=['orderID', 'Product_ID'])
    print(f"      - Stream Cleaned: Dropped {initial_count - len(stream_df)} duplicates.")

    # Clean Product: Fix Prices
    # Find price column dynamically like in the ETL
    price_col = next((c for c in product_df.columns if 'price' in c.lower()), None)
    product_df[price_col] = product_df[price_col].astype(str).str.replace('$', '', regex=False)
    product_df[price_col] = pd.to_numeric(product_df[price_col], errors='coerce').fillna(0.0)

    # MERGE (Inner Join) - This simulates the Hybrid Join
    # We use inner join because if a Product ID isn't in Master, ETL skips it.
    merged_df = pd.merge(
        stream_df, 
        product_df, 
        on='Product_ID', 
        how='inner'
    )

    # Calculate Expected Revenue
    merged_df['quantity'] = pd.to_numeric(merged_df['quantity'])
    merged_df['Calculated_Revenue'] = merged_df['quantity'] * merged_df[price_col]

    # EXPECTED METRICS
    expected_rows = len(merged_df)
    expected_revenue = round(merged_df['Calculated_Revenue'].sum(), 2)
    expected_qty = merged_df['quantity'].sum()

    print(f"      - CSV Calculation Complete.")

    # ---------------------------------------------------------
    # 2. GET ACTUAL RESULTS (FROM DATABASE)
    # ---------------------------------------------------------
    print("[2/3] Querying MySQL Database...")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Query Total Rows
    cursor.execute("SELECT COUNT(*) FROM FactSales")
    actual_rows = cursor.fetchone()[0]

    # Query Total Revenue
    cursor.execute("SELECT SUM(Revenue) FROM FactSales")
    actual_revenue = cursor.fetchone()[0]
    actual_revenue = float(actual_revenue) if actual_revenue else 0.0

    # Query Total Quantity
    cursor.execute("SELECT SUM(Quantity) FROM FactSales")
    actual_qty = cursor.fetchone()[0]
    actual_qty = int(actual_qty) if actual_qty else 0

    cursor.close()
    conn.close()

    # ---------------------------------------------------------
    # 3. COMPARISON REPORT
    # ---------------------------------------------------------
    print("\n" + "="*60)
    print("📊 FINAL AUDIT REPORT")
    print("="*60)
    
    # 1. Row Count Check
    print(f"{'METRIC':<20} | {'EXPECTED (CSV)':<15} | {'ACTUAL (DB)':<15} | {'STATUS'}")
    print("-" * 65)
    
    row_status = "✅ PASS" if expected_rows == actual_rows else "❌ FAIL"
    print(f"{'Total Rows':<20} | {expected_rows:<15} | {actual_rows:<15} | {row_status}")

    # 2. Revenue Check (Allow tiny float difference)
    rev_diff = abs(expected_revenue - actual_revenue)
    rev_status = "✅ PASS" if rev_diff < 1.0 else "❌ FAIL"
    print(f"{'Total Revenue':<20} | ${expected_revenue:,.2f}      | ${actual_revenue:,.2f}      | {rev_status}")

    # 3. Quantity Check
    qty_status = "✅ PASS" if expected_qty == actual_qty else "❌ FAIL"
    print(f"{'Total Quantity':<20} | {expected_qty:<15} | {actual_qty:<15} | {qty_status}")

    print("="*60)
    if row_status == "✅ PASS" and rev_status == "✅ PASS":
        print("🎉 SUCCESS: Data Warehouse matches Source Data exactly!")
    else:
        print("⚠️ WARNING: Discrepancies found. Check for duplicates or missing Master Data.")

if __name__ == "__main__":
    verify_data()