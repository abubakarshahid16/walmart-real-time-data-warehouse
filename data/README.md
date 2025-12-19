# Data Files

⚠️ **CSV files are not included in this repository due to size constraints.**

## Required Files

Place the following CSV files in this `data/` directory:

1. **transactional_data.csv** (~25 MB)
   - Contains: Order ID, Customer ID, Product ID, Quantity, Date
   - Records: ~550,000 transactions

2. **product_master_data.csv** (~226 KB)
   - Contains: Product ID, Category, Price, Supplier ID, Store ID
   - Records: 3,631 products

3. **customer_master_data.csv** (~182 KB)
   - Contains: Customer ID, Gender, Age, Occupation, City, Marital Status
   - Records: ~5,891 customers

## File Locations

```
data/
├── transactional_data.csv     (you provide)
├── product_master_data.csv     (you provide)
└── customer_master_data.csv    (you provide)
```

## Sample Data

A small sample dataset is available in `data/sample/` for testing purposes.

## How to Obtain Data

Contact the repository owner or generate synthetic data using the provided scripts in `scripts/generate_sample_data.py`.
