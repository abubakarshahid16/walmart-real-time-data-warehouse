-- =====================================================================
-- BLACKDW DATA WAREHOUSE - STAR SCHEMA CREATION SCRIPT (FINAL)
-- =====================================================================

-- 1. DATABASE CREATION
-- =====================================================================
CREATE DATABASE IF NOT EXISTS BlackDW;
USE BlackDW;

-- =====================================================================
-- 2. DROP EXISTING TABLES (for a clean, repeatable setup)
-- =====================================================================
DROP TABLE IF EXISTS FactSales;
DROP TABLE IF EXISTS DimProduct;
DROP TABLE IF EXISTS DimCustomer;
DROP TABLE IF EXISTS DimSupplier;
DROP TABLE IF EXISTS DimStore;
DROP TABLE IF EXISTS DimDate;

-- =====================================================================
-- 3. DIMENSION TABLES
-- =====================================================================

-- -------------------------------------------------------------
-- DimCustomer
-- -------------------------------------------------------------
CREATE TABLE DimCustomer (
    CustomerKey INT AUTO_INCREMENT PRIMARY KEY,
    Customer_ID VARCHAR(20) NOT NULL,
    Gender CHAR(1),
    Age VARCHAR(10),
    Occupation VARCHAR(50),
    City_Category CHAR(1),
    Stay_In_Current_City_Years VARCHAR(5),
    Marital_Status INT,
    UNIQUE(Customer_ID)
);

-- -------------------------------------------------------------
-- DimSupplier
-- -------------------------------------------------------------
CREATE TABLE DimSupplier (
    SupplierKey INT AUTO_INCREMENT PRIMARY KEY,
    SupplierID INT NOT NULL,
    SupplierName VARCHAR(255),
    UNIQUE(SupplierID)
);

-- -------------------------------------------------------------
-- DimStore
-- -------------------------------------------------------------
CREATE TABLE DimStore (
    StoreKey INT AUTO_INCREMENT PRIMARY KEY,
    StoreID INT NOT NULL,
    StoreName VARCHAR(255),
    City_Category CHAR(1),
    UNIQUE(StoreID)
);

-- -------------------------------------------------------------
-- DimDate (Enhanced)
-- -------------------------------------------------------------
CREATE TABLE DimDate (
    DateKey INT AUTO_INCREMENT PRIMARY KEY,
    FullDate DATE NOT NULL UNIQUE,
    Day INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    Quarter INT NOT NULL,
    Year INT NOT NULL,
    Week INT NOT NULL,
    Weekday VARCHAR(10) NOT NULL,
    Season VARCHAR(10) NOT NULL
);

-- -------------------------------------------------------------
-- DimProduct
-- Product dimension references Supplier & Store
-- -------------------------------------------------------------
CREATE TABLE DimProduct (
    ProductKey INT AUTO_INCREMENT PRIMARY KEY,
    Product_ID VARCHAR(20) NOT NULL,
    Product_Category VARCHAR(255),
    Price DECIMAL(10,2) NOT NULL,
    SupplierKey INT NOT NULL,
    StoreKey INT NOT NULL,
    UNIQUE(Product_ID),
    FOREIGN KEY (SupplierKey) REFERENCES DimSupplier(SupplierKey) ON UPDATE CASCADE ON DELETE NO ACTION,
    FOREIGN KEY (StoreKey) REFERENCES DimStore(StoreKey) ON UPDATE CASCADE ON DELETE NO ACTION
);

-- =====================================================================
-- 4. FACT TABLE
-- Includes Foreign Keys, Degenerate Dimension (Order_ID), and Measures
-- =====================================================================
CREATE TABLE FactSales (
    Sales_ID BIGINT AUTO_INCREMENT PRIMARY KEY, -- Changed to BIGINT for large volumes

    -- Foreign Keys to Dimension Tables
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    SupplierKey INT NOT NULL,
    StoreKey INT NOT NULL,
    DateKey INT NOT NULL,

    -- Degenerate Dimension
    Order_ID VARCHAR(50) NOT NULL,

    -- Measures
    Quantity INT NOT NULL,
    Revenue DECIMAL(12,2) NOT NULL, -- Increased precision

    -- Foreign Key Constraints
    FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey) ON UPDATE CASCADE ON DELETE NO ACTION,
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey) ON UPDATE CASCADE ON DELETE NO ACTION,
    FOREIGN KEY (SupplierKey) REFERENCES DimSupplier(SupplierKey) ON UPDATE CASCADE ON DELETE NO ACTION,
    FOREIGN KEY (StoreKey) REFERENCES DimStore(StoreKey) ON UPDATE CASCADE ON DELETE NO ACTION,
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey) ON UPDATE CASCADE ON DELETE NO ACTION
);

-- =====================================================================
-- 5. PERFORMANCE INDEXES
-- =====================================================================
CREATE INDEX idx_fs_customer ON FactSales(CustomerKey);
CREATE INDEX idx_fs_product  ON FactSales(ProductKey);
CREATE INDEX idx_fs_supplier ON FactSales(SupplierKey);
CREATE INDEX idx_fs_store    ON FactSales(StoreKey);
CREATE INDEX idx_fs_date     ON FactSales(DateKey);
CREATE INDEX idx_fs_order_id ON FactSales(Order_ID);

-- =====================================================================
-- SCRIPT COMPLETE
-- =====================================================================
COMMIT;