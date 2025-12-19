import streamlit as st
import pandas as pd
import time
import plotly.express as px
from sqlalchemy import create_engine
import plotly.graph_objects as go

# ==========================================
# 0. PAGE CONFIGURATION (Must be first)
# ==========================================
st.set_page_config(
    page_title="Walmart Real-Time DW",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Walmart Theme
st.markdown("""
    <style>
    .main { background-color: #F0F2F6; }
    .stApp header { background-color: #0071DC; }
    h1 { color: #0071DC; }
    h3 { color: #FFC220; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 1. CONFIGURATION
# ==========================================
DB_USER = "root"
DB_PASS = "abubakar123" 
DB_HOST = "localhost"
DB_NAME = "BlackDW"  # UPDATED DATABASE NAME

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
@st.cache_resource
def get_engine():
    """Creates a SQLAlchemy engine. Cached for performance."""
    connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    return create_engine(connection_string)

def run_query(query):
    """Runs SQL using SQLAlchemy (Clean & Fast)."""
    engine = get_engine()
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        return pd.DataFrame() # Return empty on error

def get_kpis():
    """Fetches high-level metrics for the top banner."""
    try:
        df_count = run_query("SELECT COUNT(*) as c FROM FactSales")
        count = df_count['c'][0] if not df_count.empty else 0
        
        df_rev = run_query("SELECT SUM(Revenue) as r FROM FactSales")
        revenue = df_rev['r'][0] if not df_rev.empty else 0
        revenue = revenue if revenue is not None else 0.0
        
        df_cust = run_query("SELECT COUNT(DISTINCT CustomerKey) as c FROM FactSales")
        cust = df_cust['c'][0] if not df_cust.empty else 0
        cust = cust if cust is not None else 0
        
        return count, revenue, cust
    except:
        return 0, 0, 0

# ==========================================
# 3. QUERY LIBRARY (ALL 20 QUERIES)
# ==========================================
QUERIES = {
    "Q1: Top 5 Products (Weekday vs Weekend)": """
        WITH ProductSales AS (
            SELECT p.Product_ID, d.Year, d.MonthName,
            CASE WHEN d.Weekday IN ('Saturday', 'Sunday') THEN 'Weekend' ELSE 'Weekday' END AS DayType,
            SUM(fs.Revenue) AS TotalRevenue,
            RANK() OVER(PARTITION BY d.Year, d.MonthName, (CASE WHEN d.Weekday IN ('Saturday', 'Sunday') THEN 'Weekend' ELSE 'Weekday' END) ORDER BY SUM(fs.Revenue) DESC) as RankNum
            FROM FactSales fs
            JOIN DimProduct p ON fs.ProductKey = p.ProductKey
            JOIN DimDate d ON fs.DateKey = d.DateKey
            WHERE d.Year = 2017
            GROUP BY p.Product_ID, d.Year, d.MonthName, DayType
        )
        SELECT Year, MonthName, DayType, Product_ID, TotalRevenue
        FROM ProductSales WHERE RankNum <= 5
        ORDER BY Year, MonthName, DayType, TotalRevenue DESC;
    """,
    "Q2: Customer Demographics by Purchase": """
        SELECT c.Gender, c.Age, c.City_Category, SUM(fs.Revenue) AS TotalPurchaseAmount
        FROM FactSales fs
        JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
        GROUP BY c.Gender, c.Age, c.City_Category
        ORDER BY c.City_Category, c.Gender, c.Age;
    """,
    "Q3: Product Category Sales by Occupation": """
        SELECT p.Product_Category, c.Occupation, SUM(fs.Revenue) AS TotalSales
        FROM FactSales fs
        JOIN DimProduct p ON fs.ProductKey = p.ProductKey
        JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
        GROUP BY p.Product_Category, c.Occupation
        ORDER BY p.Product_Category, TotalSales DESC;
    """,
    "Q4: Total Purchases by Gender/Age (Quarterly)": """
        SELECT c.Gender, c.Age, d.Year, d.Quarter, SUM(fs.Revenue) AS TotalPurchaseAmount
        FROM FactSales fs
        JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
        JOIN DimDate d ON fs.DateKey = d.DateKey
        WHERE d.Year = 2017
        GROUP BY c.Gender, c.Age, d.Year, d.Quarter
        ORDER BY d.Year, d.Quarter, c.Gender, c.Age;
    """,
    "Q5: Top 5 Occupations by Category": """
        WITH OccupationSales AS (
            SELECT p.Product_Category, c.Occupation, SUM(fs.Revenue) AS TotalSales,
            RANK() OVER(PARTITION BY p.Product_Category ORDER BY SUM(fs.Revenue) DESC) as RankNum
            FROM FactSales fs
            JOIN DimProduct p ON fs.ProductKey = p.ProductKey
            JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
            GROUP BY p.Product_Category, c.Occupation
        )
        SELECT Product_Category, Occupation, TotalSales
        FROM OccupationSales WHERE RankNum <= 5
        ORDER BY Product_Category, TotalSales DESC;
    """,
    "Q6: City Performance by Marital Status": """
        SELECT c.City_Category,
        CASE c.Marital_Status WHEN 1 THEN 'Married' ELSE 'Single' END AS MaritalStatus,
        d.Year, d.MonthName, SUM(fs.Revenue) AS TotalPurchaseAmount
        FROM FactSales fs
        JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
        JOIN DimDate d ON fs.DateKey = d.DateKey
        GROUP BY c.City_Category, MaritalStatus, d.Year, d.MonthName
        ORDER BY c.City_Category, d.Year, d.MonthName;
    """,
    "Q7: Avg Purchase by Stay Duration": """
        SELECT c.Stay_In_Current_City_Years, c.Gender, AVG(fs.Revenue) AS AveragePurchaseAmount
        FROM FactSales fs
        JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
        GROUP BY c.Stay_In_Current_City_Years, c.Gender
        ORDER BY c.Stay_In_Current_City_Years, c.Gender;
    """,
    "Q8: Top 5 Cities by Category": """
        WITH CitySales AS (
            SELECT p.Product_Category, c.City_Category, SUM(fs.Revenue) AS TotalRevenue,
            RANK() OVER(PARTITION BY p.Product_Category ORDER BY SUM(fs.Revenue) DESC) as RankNum
            FROM FactSales fs
            JOIN DimProduct p ON fs.ProductKey = p.ProductKey
            JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
            GROUP BY p.Product_Category, c.City_Category
        )
        SELECT Product_Category, City_Category, TotalRevenue
        FROM CitySales WHERE RankNum <= 5
        ORDER BY Product_Category, TotalRevenue DESC;
    """,
    "Q9: Monthly Sales Growth": """
        WITH MonthlySales AS (
            SELECT p.Product_Category, d.Year, d.Month, SUM(fs.Revenue) AS MonthlyRevenue
            FROM FactSales fs
            JOIN DimProduct p ON fs.ProductKey = p.ProductKey
            JOIN DimDate d ON fs.DateKey = d.DateKey
            WHERE d.Year = 2017
            GROUP BY p.Product_Category, d.Year, d.Month
        ),
        LaggedSales AS (
            SELECT Product_Category, Year, Month, MonthlyRevenue,
            LAG(MonthlyRevenue, 1, 0) OVER(PARTITION BY Product_Category ORDER BY Year, Month) AS PreviousMonthRevenue
            FROM MonthlySales
        )
        SELECT Product_Category, Year, Month, MonthlyRevenue, PreviousMonthRevenue,
        (MonthlyRevenue - PreviousMonthRevenue) / PreviousMonthRevenue * 100 AS GrowthPercentage
        FROM LaggedSales WHERE PreviousMonthRevenue > 0;
    """,
    "Q10: Weekend vs Weekday by Age": """
        SELECT c.Age,
        CASE WHEN d.Weekday IN ('Saturday', 'Sunday') THEN 'Weekend' ELSE 'Weekday' END AS DayType,
        SUM(fs.Revenue) AS TotalSales
        FROM FactSales fs
        JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
        JOIN DimDate d ON fs.DateKey = d.DateKey
        WHERE d.Year = 2017
        GROUP BY c.Age, DayType
        ORDER BY c.Age, DayType;
    """,
    "Q11: Top 5 Products (Same as Q1)": """
        WITH ProductSales AS (
            SELECT p.Product_ID, d.Year, d.MonthName,
            CASE WHEN d.Weekday IN ('Saturday', 'Sunday') THEN 'Weekend' ELSE 'Weekday' END AS DayType,
            SUM(fs.Revenue) AS TotalRevenue,
            RANK() OVER(PARTITION BY d.Year, d.MonthName, (CASE WHEN d.Weekday IN ('Saturday', 'Sunday') THEN 'Weekend' ELSE 'Weekday' END) ORDER BY SUM(fs.Revenue) DESC) as RankNum
            FROM FactSales fs
            JOIN DimProduct p ON fs.ProductKey = p.ProductKey
            JOIN DimDate d ON fs.DateKey = d.DateKey
            WHERE d.Year = 2017
            GROUP BY p.Product_ID, d.Year, d.MonthName, DayType
        )
        SELECT Year, MonthName, DayType, Product_ID, TotalRevenue
        FROM ProductSales WHERE RankNum <= 5;
    """,
    "Q12: Quarterly Store Growth": """
        WITH QuarterlyStoreSales AS (
            SELECT s.StoreName, d.Quarter, SUM(fs.Revenue) AS QuarterlyRevenue
            FROM FactSales fs
            JOIN DimStore s ON fs.StoreKey = s.StoreKey
            JOIN DimDate d ON fs.DateKey = d.DateKey
            WHERE d.Year = 2017
            GROUP BY s.StoreName, d.Quarter
        ),
        LaggedQuarterlySales AS (
            SELECT StoreName, Quarter, QuarterlyRevenue,
            LAG(QuarterlyRevenue, 1, 0) OVER(PARTITION BY StoreName ORDER BY Quarter) AS PreviousQuarterRevenue
            FROM QuarterlyStoreSales
        )
        SELECT StoreName, Quarter, QuarterlyRevenue, PreviousQuarterRevenue,
        (QuarterlyRevenue - PreviousQuarterRevenue) / PreviousQuarterRevenue * 100 AS GrowthRate
        FROM LaggedQuarterlySales WHERE PreviousQuarterRevenue > 0
        ORDER BY StoreName, Quarter;
    """,
    "Q13: Supplier Sales by Store": """
        SELECT st.StoreName, sp.SupplierName, p.Product_ID, SUM(fs.Revenue) AS TotalSales
        FROM FactSales fs
        JOIN DimStore st ON fs.StoreKey = st.StoreKey
        JOIN DimSupplier sp ON fs.SupplierKey = sp.SupplierKey
        JOIN DimProduct p ON fs.ProductKey = p.ProductKey
        GROUP BY st.StoreName, sp.SupplierName, p.Product_ID
        ORDER BY st.StoreName, sp.SupplierName, TotalSales DESC;
    """,
    "Q14: Seasonal Product Analysis": """
        SELECT p.Product_Category, d.Season, SUM(fs.Revenue) AS TotalSales
        FROM FactSales fs
        JOIN DimProduct p ON fs.ProductKey = p.ProductKey
        JOIN DimDate d ON fs.DateKey = d.DateKey
        GROUP BY p.Product_Category, d.Season
        ORDER BY p.Product_Category, d.Season;
    """,
    "Q15: Monthly Volatility (Store & Supplier)": """
        WITH MonthlyPairSales AS (
            SELECT s.StoreName, sp.SupplierName, d.Year, d.Month, SUM(fs.Revenue) AS MonthlyRevenue
            FROM FactSales fs
            JOIN DimStore s ON fs.StoreKey = s.StoreKey
            JOIN DimSupplier sp ON fs.SupplierKey = sp.SupplierKey
            JOIN DimDate d ON fs.DateKey = d.DateKey
            GROUP BY s.StoreName, sp.SupplierName, d.Year, d.Month
        ),
        LaggedPairSales AS (
            SELECT StoreName, SupplierName, Year, Month, MonthlyRevenue,
            LAG(MonthlyRevenue, 1, 0) OVER(PARTITION BY StoreName, SupplierName ORDER BY Year, Month) AS PreviousMonthRevenue
            FROM MonthlyPairSales
        )
        SELECT StoreName, SupplierName, Year, Month,
        (MonthlyRevenue - PreviousMonthRevenue) / PreviousMonthRevenue * 100 AS VolatilityPercentage
        FROM LaggedPairSales WHERE PreviousMonthRevenue > 0
        ORDER BY StoreName, SupplierName, Year, Month;
    """,
    "Q16: Product Affinity (Bought Together)": """
        WITH ProductPairs AS (
            SELECT fs1.ProductKey AS Product1, fs2.ProductKey AS Product2,
            COUNT(DISTINCT fs1.Order_ID) AS TimesBoughtTogether
            FROM FactSales fs1
            JOIN FactSales fs2 ON fs1.Order_ID = fs2.Order_ID AND fs1.ProductKey < fs2.ProductKey
            GROUP BY fs1.ProductKey, fs2.ProductKey
        )
        SELECT p1.Product_ID AS Product1_ID, p2.Product_ID AS Product2_ID, pp.TimesBoughtTogether
        FROM ProductPairs pp
        JOIN DimProduct p1 ON pp.Product1 = p1.ProductKey
        JOIN DimProduct p2 ON pp.Product2 = p2.ProductKey
        ORDER BY pp.TimesBoughtTogether DESC
        LIMIT 5;
    """,
    "Q17: Yearly Revenue Rollup": """
        SELECT IFNULL(s.StoreName, 'All Stores') AS StoreName,
        IFNULL(sp.SupplierName, 'All Suppliers') AS SupplierName,
        IFNULL(p.Product_ID, 'All Products') AS Product_ID,
        d.Year, SUM(fs.Revenue) AS TotalRevenue
        FROM FactSales fs
        JOIN DimStore s ON fs.StoreKey = s.StoreKey
        JOIN DimSupplier sp ON fs.SupplierKey = sp.SupplierKey
        JOIN DimProduct p ON fs.ProductKey = p.ProductKey
        JOIN DimDate d ON fs.DateKey = d.DateKey
        GROUP BY d.Year, s.StoreName, sp.SupplierName, p.Product_ID WITH ROLLUP;
    """,
    "Q18: H1 vs H2 Revenue Analysis": """
        SELECT p.Product_ID,
        SUM(CASE WHEN d.Month <= 6 THEN fs.Revenue ELSE 0 END) AS H1_Revenue,
        SUM(CASE WHEN d.Month <= 6 THEN fs.Quantity ELSE 0 END) AS H1_Quantity,
        SUM(CASE WHEN d.Month > 6 THEN fs.Revenue ELSE 0 END) AS H2_Revenue,
        SUM(CASE WHEN d.Month > 6 THEN fs.Quantity ELSE 0 END) AS H2_Quantity,
        SUM(fs.Revenue) AS TotalYearlyRevenue,
        SUM(fs.Quantity) AS TotalYearlyQuantity
        FROM FactSales fs
        JOIN DimProduct p ON fs.ProductKey = p.ProductKey
        JOIN DimDate d ON fs.DateKey = d.DateKey
        GROUP BY p.Product_ID
        ORDER BY TotalYearlyRevenue DESC;
    """,
    "Q19: High Revenue Spikes (Outliers)": """
        WITH DailyProductSales AS (
            SELECT ProductKey, DateKey, SUM(Revenue) as DailyRevenue
            FROM FactSales GROUP BY ProductKey, DateKey
        ),
        AvgProductSales AS (
            SELECT ProductKey, AVG(DailyRevenue) as AvgDailyRevenue
            FROM DailyProductSales GROUP BY ProductKey
        )
        SELECT dps.DateKey, dp.Product_ID, dps.DailyRevenue, aps.AvgDailyRevenue
        FROM DailyProductSales dps
        JOIN AvgProductSales aps ON dps.ProductKey = aps.ProductKey
        JOIN DimProduct dp ON dps.ProductKey = dp.ProductKey
        WHERE dps.DailyRevenue > (2 * aps.AvgDailyRevenue)
        ORDER BY dps.DailyRevenue DESC;
    """,
    "Q20: Quarterly Sales View": """
        SELECT * FROM STORE_QUARTERLY_SALES
    """
}

# ==========================================
# 4. SIDEBAR & TITLE
# ==========================================
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/c/ca/Walmart_logo.svg/2560px-Walmart_logo.svg.png", width=200)
    st.markdown("### ⚙️ System Control")
    st.info(f"Connected to: **{DB_NAME}**")
    st.caption("Status: Dashboard Ready")

st.title("🛒 Walmart Real-Time Data Warehouse")

# TABS
tab1, tab2 = st.tabs(["🚀 Real-Time ETL Monitor", "📊 OLAP Analysis (20 Queries)"])

# =========================================================
# IMPORTANT: WE DEFINE ALL TABS *BEFORE* THE INFINITE LOOP
# =========================================================

# --- TAB 1 LAYOUT ---
with tab1:
    st.subheader("Live Data Stream")
    col1, col2, col3 = st.columns(3)
    kpi1 = col1.empty()
    kpi2 = col2.empty()
    kpi3 = col3.empty()
    
    col_c1, col_c2 = st.columns([1, 4])
    run_monitoring = col_c1.toggle("🔴 Start Live Monitoring", value=False)
    
    chart_placeholder = st.empty()

# --- TAB 2 LAYOUT ---
with tab2:
    st.subheader("Business Intelligence & OLAP")
    
    if run_monitoring:
        st.warning("⚠️ STOP Live Monitoring in Tab 1 to run queries! (Database is busy)")
        
    col_sel, col_btn = st.columns([3, 1])
    with col_sel:
        query_option = st.selectbox("Select Business Question:", list(QUERIES.keys()))
    with col_btn:
        st.write("") 
        st.write("") 
        run_btn = st.button("Run Analysis", type="primary", disabled=run_monitoring)

    if run_btn:
        sql = QUERIES[query_option]
        with st.spinner('Querying Data Warehouse...'):
            df_result = run_query(sql)
            
        if not df_result.empty:
            st.success(f"Retrieved {len(df_result)} rows.")
            
            # === INTELLIGENT VISUALIZATION (FINAL FIXED VERSION) ===
            cols = df_result.columns.tolist()
            
            # 1. Identify Numeric Metrics
            num_cols = df_result.select_dtypes(include=['number']).columns.tolist()
            
            # FIX: Exclude ID/Dimension columns from being plotted as metrics
            valid_metrics = [c for c in num_cols if c not in [
                'Year', 'Quarter', 'Month', 'Day', 'Week', 'RankNum',
                'Marital_Status', 'Stay_In_Current_City_Years'
            ]]
            
            # 2. Identify Categorical Columns
            cat_cols = df_result.select_dtypes(include=['object', 'string']).columns.tolist()
            
            # Force specific columns to be Category even if they look like numbers
            for c in ['Stay_In_Current_City_Years', 'Year', 'Quarter']:
                if c in num_cols: cat_cols.append(c)

            # Visualization Logic
            fig = None
            
            # Case 1: Time Series
            if any(c in cols for c in ['MonthName', 'FullDate', 'Year', 'Quarter']) and len(valid_metrics) > 0:
                 x_col = next((c for c in cols if c in ['FullDate', 'MonthName', 'Quarter', 'Year']), cols[0])
                 y_col = valid_metrics[0] 
                 color_col = next((c for c in cat_cols if c != x_col), None)
                 
                 fig = px.line(df_result, x=x_col, y=y_col, color=color_col, markers=True, 
                               title=f"Trend Analysis: {y_col} over {x_col}")

            # Case 2: Comparison (Bar Chart)
            elif (len(cat_cols) > 0 or 'Stay_In_Current_City_Years' in cols) and len(valid_metrics) > 0:
                # Prioritize Stay Years or City Category for X-axis
                x_col = cat_cols[0] if cat_cols else cols[0]
                y_col = valid_metrics[0]
                color_col = next((c for c in cat_cols if c != x_col), None)
                
                fig = px.bar(df_result, x=x_col, y=y_col, color=color_col, barmode='group',
                             title=f"Comparison: {y_col} by {x_col}")
            
            # Apply Walmart Colors & Render
            if fig:
                fig.update_layout(plot_bgcolor="white")
                st.plotly_chart(fig, width="stretch")
            
            with st.expander("View Data"):
                st.dataframe(df_result)
        else:
            st.warning("No data found.")

# ==========================================
# 5. EXECUTION LOGIC
# ==========================================
if run_monitoring:
    while True:
        count, rev, cust = get_kpis()
        
        with kpi1.container():
            st.metric("Total Transactions", f"{count:,}")
        with kpi2.container():
            st.metric("Total Revenue", f"${rev:,.2f}")
        with kpi3.container():
            st.metric("Unique Customers", f"{cust:,}")
        
        df_recent = run_query("SELECT Sales_ID, Revenue FROM FactSales ORDER BY Sales_ID DESC LIMIT 100")
        if not df_recent.empty:
            fig = px.area(df_recent, x='Sales_ID', y='Revenue', 
                          title="Incoming Sales Stream (Last 100)",
                          color_discrete_sequence=['#0071DC'])
            fig.update_layout(plot_bgcolor="white", height=350, margin=dict(l=20, r=20, t=40, b=20))
            
            chart_placeholder.plotly_chart(fig, width="stretch", key=f"live_{time.time()}")
        
        time.sleep(1.5)