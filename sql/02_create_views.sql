-- =====================================================================
-- WALMART DATA WAREHOUSE - ANALYSIS QUERIES
-- =====================================================================
CREATE DATABASE BLACKDW;
USE BlackDW;

--- Q1: Top 5 Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down ---
WITH ProductSales AS (
    SELECT
        p.Product_ID,
        d.Year,
        d.MonthName,
        CASE WHEN d.Weekday IN ('Saturday', 'Sunday') THEN 'Weekend' ELSE 'Weekday' END AS DayType,
        SUM(fs.Revenue) AS TotalRevenue,
        RANK() OVER(PARTITION BY d.Year, d.MonthName, (CASE WHEN d.Weekday IN ('Saturday', 'Sunday') THEN 'Weekend' ELSE 'Weekday' END) ORDER BY SUM(fs.Revenue) DESC) as RankNum
    FROM FactSales fs
    JOIN DimProduct p ON fs.ProductKey = p.ProductKey
    JOIN DimDate d ON fs.DateKey = d.DateKey
    WHERE d.Year = 2017 -- Assuming analysis for a specific year, e.g., 2017
    GROUP BY p.Product_ID, d.Year, d.MonthName, DayType
)
SELECT Year, MonthName, DayType, Product_ID, TotalRevenue
FROM ProductSales
WHERE RankNum <= 5
ORDER BY Year, MonthName, DayType, TotalRevenue DESC;


--- Q2: Customer Demographics by Purchase Amount with City Category Breakdown ---
SELECT
    c.Gender,
    c.Age,
    c.City_Category,
    SUM(fs.Revenue) AS TotalPurchaseAmount
FROM FactSales fs
JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
GROUP BY c.Gender, c.Age, c.City_Category
ORDER BY c.City_Category, c.Gender, c.Age;


--- Q3: Product Category Sales by Occupation ---
SELECT
    p.Product_Category,
    c.Occupation,
    SUM(fs.Revenue) AS TotalSales
FROM FactSales fs
JOIN DimProduct p ON fs.ProductKey = p.ProductKey
JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
GROUP BY p.Product_Category, c.Occupation
ORDER BY p.Product_Category, TotalSales DESC;


--- Q4: Total Purchases by Gender and Age Group with Quarterly Trend ---
SELECT
    c.Gender,
    c.Age,
    d.Year,
    d.Quarter,
    SUM(fs.Revenue) AS TotalPurchaseAmount
FROM FactSales fs
JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
JOIN DimDate d ON fs.DateKey = d.DateKey
WHERE d.Year = 2017 -- Assuming current year
GROUP BY c.Gender, c.Age, d.Year, d.Quarter
ORDER BY d.Year, d.Quarter, c.Gender, c.Age;


--- Q5: Top 5 Occupations by Product Category Sales ---
WITH OccupationSales AS (
    SELECT
        p.Product_Category,
        c.Occupation,
        SUM(fs.Revenue) AS TotalSales,
        RANK() OVER(PARTITION BY p.Product_Category ORDER BY SUM(fs.Revenue) DESC) as RankNum
    FROM FactSales fs
    JOIN DimProduct p ON fs.ProductKey = p.ProductKey
    JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
    GROUP BY p.Product_Category, c.Occupation
)
SELECT Product_Category, Occupation, TotalSales
FROM OccupationSales
WHERE RankNum <= 5
ORDER BY Product_Category, TotalSales DESC;


--- Q6: City Category Performance by Marital Status with Monthly Breakdown (Last 6 Months) ---
SELECT
    c.City_Category,
    CASE c.Marital_Status WHEN 1 THEN 'Married' ELSE 'Single' END AS MaritalStatus,
    d.Year,
    d.MonthName,
    SUM(fs.Revenue) AS TotalPurchaseAmount
FROM FactSales fs
JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
JOIN DimDate d ON fs.DateKey = d.DateKey
WHERE d.FullDate >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
GROUP BY c.City_Category, MaritalStatus, d.Year, d.MonthName
ORDER BY c.City_Category, d.Year, d.MonthName;


--- Q7: Average Purchase Amount by Stay Duration and Gender ---
SELECT
    c.Stay_In_Current_City_Years,
    c.Gender,
    AVG(fs.Revenue) AS AveragePurchaseAmount
FROM FactSales fs
JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
GROUP BY c.Stay_In_Current_City_Years, c.Gender
ORDER BY c.Stay_In_Current_City_Years, c.Gender;


--- Q8: Top 5 Revenue-Generating Cities by Product Category ---
WITH CitySales AS (
    SELECT
        p.Product_Category,
        c.City_Category,
        SUM(fs.Revenue) AS TotalRevenue,
        RANK() OVER(PARTITION BY p.Product_Category ORDER BY SUM(fs.Revenue) DESC) as RankNum
    FROM FactSales fs
    JOIN DimProduct p ON fs.ProductKey = p.ProductKey
    JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
    GROUP BY p.Product_Category, c.City_Category
)
SELECT Product_Category, City_Category, TotalRevenue
FROM CitySales
WHERE RankNum <= 5
ORDER BY Product_Category, TotalRevenue DESC;


--- Q9: Monthly Sales Growth by Product Category ---
WITH MonthlySales AS (
    SELECT
        p.Product_Category,
        d.Year,
        d.Month,
        SUM(fs.Revenue) AS MonthlyRevenue
    FROM FactSales fs
    JOIN DimProduct p ON fs.ProductKey = p.ProductKey
    JOIN DimDate d ON fs.DateKey = d.DateKey
    WHERE d.Year = 2017 -- Assuming current year
    GROUP BY p.Product_Category, d.Year, d.Month
),
LaggedSales AS (
    SELECT
        Product_Category,
        Year,
        Month,
        MonthlyRevenue,
        LAG(MonthlyRevenue, 1, 0) OVER(PARTITION BY Product_Category ORDER BY Year, Month) AS PreviousMonthRevenue
    FROM MonthlySales
)
SELECT
    Product_Category,
    Year,
    Month,
    MonthlyRevenue,
    PreviousMonthRevenue,
    (MonthlyRevenue - PreviousMonthRevenue) / PreviousMonthRevenue * 100 AS GrowthPercentage
FROM LaggedSales
WHERE PreviousMonthRevenue > 0;


--- Q10: Weekend vs. Weekday Sales by Age Group ---
SELECT
    c.Age,
    CASE WHEN d.Weekday IN ('Saturday', 'Sunday') THEN 'Weekend' ELSE 'Weekday' END AS DayType,
    SUM(fs.Revenue) AS TotalSales
FROM FactSales fs
JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey
JOIN DimDate d ON fs.DateKey = d.DateKey
WHERE d.Year = 2017 -- Assuming current year
GROUP BY c.Age, DayType
ORDER BY c.Age, DayType;


--- Q11: This is a duplicate of Q1. ---


--- Q12: Trend Analysis of Store Revenue Growth Rate Quarterly for 2017 ---
WITH QuarterlyStoreSales AS (
    SELECT
        s.StoreName,
        d.Quarter,
        SUM(fs.Revenue) AS QuarterlyRevenue
    FROM FactSales fs
    JOIN DimStore s ON fs.StoreKey = s.StoreKey
    JOIN DimDate d ON fs.DateKey = d.DateKey
    WHERE d.Year = 2017
    GROUP BY s.StoreName, d.Quarter
),
LaggedQuarterlySales AS (
    SELECT
        StoreName,
        Quarter,
        QuarterlyRevenue,
        LAG(QuarterlyRevenue, 1, 0) OVER(PARTITION BY StoreName ORDER BY Quarter) AS PreviousQuarterRevenue
    FROM QuarterlyStoreSales
)
SELECT
    StoreName,
    Quarter,
    QuarterlyRevenue,
    PreviousQuarterRevenue,
    (QuarterlyRevenue - PreviousQuarterRevenue) / PreviousQuarterRevenue * 100 AS GrowthRate
FROM LaggedQuarterlySales
WHERE PreviousQuarterRevenue > 0
ORDER BY StoreName, Quarter;


--- Q13: Detailed Supplier Sales Contribution by Store and Product Name ---
SELECT
    st.StoreName,
    sp.SupplierName,
    p.Product_ID,
    SUM(fs.Revenue) AS TotalSales
FROM FactSales fs
JOIN DimStore st ON fs.StoreKey = st.StoreKey
JOIN DimSupplier sp ON fs.SupplierKey = sp.SupplierKey
JOIN DimProduct p ON fs.ProductKey = p.ProductKey
GROUP BY st.StoreName, sp.SupplierName, p.Product_ID
ORDER BY st.StoreName, sp.SupplierName, TotalSales DESC;


--- Q14: Seasonal Analysis of Product Sales Using Dynamic Drill-Down ---
SELECT
    p.Product_Category,
    d.Season,
    SUM(fs.Revenue) AS TotalSales
FROM FactSales fs
JOIN DimProduct p ON fs.ProductKey = p.ProductKey
JOIN DimDate d ON fs.DateKey = d.DateKey
GROUP BY p.Product_Category, d.Season
ORDER BY p.Product_Category, d.Season;


--- Q15: Store-Wise and Supplier-Wise Monthly Revenue Volatility ---
WITH MonthlyPairSales AS (
    SELECT
        s.StoreName,
        sp.SupplierName,
        d.Year,
        d.Month,
        SUM(fs.Revenue) AS MonthlyRevenue
    FROM FactSales fs
    JOIN DimStore s ON fs.StoreKey = s.StoreKey
    JOIN DimSupplier sp ON fs.SupplierKey = sp.SupplierKey
    JOIN DimDate d ON fs.DateKey = d.DateKey
    GROUP BY s.StoreName, sp.SupplierName, d.Year, d.Month
),
LaggedPairSales AS (
    SELECT
        StoreName,
        SupplierName,
        Year,
        Month,
        MonthlyRevenue,
        LAG(MonthlyRevenue, 1, 0) OVER(PARTITION BY StoreName, SupplierName ORDER BY Year, Month) AS PreviousMonthRevenue
    FROM MonthlyPairSales
)
SELECT
    StoreName,
    SupplierName,
    Year,
    Month,
    (MonthlyRevenue - PreviousMonthRevenue) / PreviousMonthRevenue * 100 AS VolatilityPercentage
FROM LaggedPairSales
WHERE PreviousMonthRevenue > 0
ORDER BY StoreName, SupplierName, Year, Month;


--- Q16: Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis) ---
WITH ProductPairs AS (
    SELECT
        fs1.ProductKey AS Product1,
        fs2.ProductKey AS Product2,
        COUNT(DISTINCT fs1.Order_ID) AS TimesBoughtTogether
    FROM FactSales fs1
    JOIN FactSales fs2 ON fs1.Order_ID = fs2.Order_ID AND fs1.ProductKey < fs2.ProductKey
    GROUP BY fs1.ProductKey, fs2.ProductKey
)
SELECT
    p1.Product_ID AS Product1_ID,
    p2.Product_ID AS Product2_ID,
    pp.TimesBoughtTogether
FROM ProductPairs pp
JOIN DimProduct p1 ON pp.Product1 = p1.ProductKey
JOIN DimProduct p2 ON pp.Product2 = p2.ProductKey
ORDER BY pp.TimesBoughtTogether DESC
LIMIT 5;


--- Q17: Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP ---
SELECT
    IFNULL(s.StoreName, 'All Stores') AS StoreName,
    IFNULL(sp.SupplierName, 'All Suppliers') AS SupplierName,
    IFNULL(p.Product_ID, 'All Products') AS Product_ID,
    d.Year,
    SUM(fs.Revenue) AS TotalRevenue
FROM FactSales fs
JOIN DimStore s ON fs.StoreKey = s.StoreKey
JOIN DimSupplier sp ON fs.SupplierKey = sp.SupplierKey
JOIN DimProduct p ON fs.ProductKey = p.ProductKey
JOIN DimDate d ON fs.DateKey = d.DateKey
GROUP BY d.Year, s.StoreName, sp.SupplierName, p.Product_ID WITH ROLLUP;


--- Q18: Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2 ---
SELECT
    p.Product_ID,
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


--- Q19: Identify High Revenue Spikes in Product Sales and Highlight Outliers ---
WITH DailyProductSales AS (
    SELECT
        ProductKey,
        DateKey,
        SUM(Revenue) as DailyRevenue
    FROM FactSales
    GROUP BY ProductKey, DateKey
),
AvgProductSales AS (
    SELECT
        ProductKey,
        AVG(DailyRevenue) as AvgDailyRevenue
    FROM DailyProductSales
    GROUP BY ProductKey
)
SELECT
    dps.DateKey,
    dp.Product_ID,
    dps.DailyRevenue,
    aps.AvgDailyRevenue
FROM DailyProductSales dps
JOIN AvgProductSales aps ON dps.ProductKey = aps.ProductKey
JOIN DimProduct dp ON dps.ProductKey = dp.ProductKey
WHERE dps.DailyRevenue > (2 * aps.AvgDailyRevenue)
ORDER BY dps.DailyRevenue DESC;


--- Q20: Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis ---
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
SELECT
    s.StoreName,
    d.Year,
    d.Quarter,
    SUM(fs.Revenue) AS TotalSales,
    SUM(fs.Quantity) AS TotalQuantity
FROM FactSales fs
JOIN DimStore s ON fs.StoreKey = s.StoreKey
JOIN DimDate d ON fs.DateKey = d.DateKey
GROUP BY s.StoreName, d.Year, d.Quarter
ORDER BY s.StoreName, d.Year, d.Quarter;

-- You can test the view with:
-- SELECT * FROM STORE_QUARTERLY_SALES;