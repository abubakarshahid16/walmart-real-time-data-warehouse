# Walmart Real-Time Data Warehouse with Hybrid Join Algorithm

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![MySQL](https://img.shields.io/badge/MySQL-8.0+-orange.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.51+-red.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

A production-ready, real-time data warehouse system implementing the **Hybrid Join algorithm** for efficient stream processing. Processes 550K+ transactions with interactive dashboards and 20+ OLAP queries.

![Architecture](assets/architecture.png)

## 🚀 Key Features

- **Hybrid Join Algorithm** - Novel approach for efficient stream-to-disk data joining
- **Real-Time ETL** - Process streaming sales data with optimized performance
- **Star Schema** - Optimized data warehouse design for analytics
- **Interactive Dashboard** - Streamlit-based visualization with live metrics
- **20+ OLAP Queries** - Pre-built business intelligence queries
- **Performance Optimizations** - RAM caching, batch commits, partitioning

## 📊 Performance Metrics

- **Processing Speed**: ~1,000 transactions/second
- **Data Volume**: 550,000+ transactions
- **ETL Time**: 8-12 minutes for full dataset
- **Query Response**: <2 seconds for most OLAP queries

## 🛠️ Tech Stack

- **Database**: MySQL 8.0+
- **Backend**: Python 3.11
- **ETL**: Hybrid Join Algorithm (custom implementation)
- **Frontend**: Streamlit
- **Visualization**: Plotly
- **Libraries**: pandas, mysql-connector-python, SQLAlchemy

## 📁 Project Structure

```
walmart-realtime-datawarehouse/
├── src/
│   ├── etl/hybrid_join_etl.py        # Main ETL engine
│   └── dashboard/streamlit_app.py     # Dashboard application
├── sql/
│   ├── 01_create_schema.sql           # Database schema
│   └── 02_create_views.sql            # OLAP views
├── data/                              # Place CSV files here
│   ├── transactional_data.csv
│   ├── product_master_data.csv
│   └── customer_master_data.csv
├── notebooks/
│   └── hybrid_join_analysis.ipynb     # Jupyter analysis
├── docs/                              # Documentation
└── scripts/                           # Utility scripts
```

## ⚡ Quick Start

### Prerequisites
- MySQL Server 8.0+
- Python 3.11+
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/walmart-realtime-datawarehouse.git
cd walmart-realtime-datawarehouse

# Install dependencies
pip install -r requirements.txt

# Setup database
mysql -u root -p < sql/01_create_schema.sql
mysql -u root -p BlackDW < sql/02_create_views.sql

# Configure database credentials
# Edit src/etl/hybrid_join_etl.py and src/dashboard/streamlit_app.py
# Update: host, user, password

# Place your data files in data/ directory
```

### Run ETL

```bash
python src/etl/hybrid_join_etl.py
```

### Launch Dashboard

```bash
streamlit run src/dashboard/streamlit_app.py
```

Access dashboard at: http://localhost:8501

## 📖 Documentation

- [Architecture Overview](docs/ARCHITECTURE.md) - System design and data flow
- [Database Schema](docs/DATABASE_SCHEMA.md) - Star schema documentation
- [Setup Guide](docs/SETUP_GUIDE.md) - Detailed installation instructions
- [User Guide](docs/USER_GUIDE.md) - Dashboard usage

## 🧮 Hybrid Join Algorithm

The Hybrid Join algorithm efficiently joins streaming data with disk-resident master data:

```
1. Buffer stream tuples in hash table (memory)
2. Partition master data into disk blocks
3. Load partition → Join with all buffered tuples
4. Amortize disk I/O cost across multiple joins
```

**Benefits**:
- 99.6% reduction in disk I/O
- O(1) hash table lookups
- Fair FIFO processing
- Scalable to large datasets

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed explanation.

## 📊 Dashboard Features

### Real-Time Monitor
- Live transaction metrics
- Revenue tracking
- Customer analytics
- Stream visualization

### OLAP Analysis (20 Queries)
- Product performance (Top N, trends)
- Customer segmentation (demographics, behavior)
- Temporal analysis (quarterly, seasonal)
- Category insights (by occupation, city)
- Store performance metrics
- Product affinity analysis

## 🗃️ Database Schema

**Star Schema** with 5 dimension tables and 1 fact table:

- **DimCustomer** - Customer demographics
- **DimProduct** - Product catalog
- **DimStore** - Store locations
- **DimSupplier** - Supplier information
- **DimDate** - Time dimension
- **FactSales** - Transaction facts (550K+ records)

## 🔧 Configuration

Edit database credentials in:
- `src/etl/hybrid_join_etl.py` (line 331)
- `src/dashboard/streamlit_app.py` (lines 31-34)

```python
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "your_password",
    "database": "BlackDW"
}
```

## 📈 Sample Queries

```sql
-- Top 5 Products by Revenue
SELECT p.Product_ID, SUM(fs.Revenue) AS TotalRevenue
FROM FactSales fs
JOIN DimProduct p ON fs.ProductKey = p.ProductKey
GROUP BY p.Product_ID
ORDER BY TotalRevenue DESC
LIMIT 5;

-- Monthly Sales Growth
SELECT d.Year, d.Month, 
       SUM(fs.Revenue) AS MonthlyRevenue
FROM FactSales fs
JOIN DimDate d ON fs.DateKey = d.DateKey
GROUP BY d.Year, d.Month
ORDER BY d.Year, d.Month;
```

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request



## 👨‍💻 Author

Abubakar
- LinkedIn:https://www.linkedin.com/in/abubakar-shahid-90a365220/)

## 🙏 Acknowledgments

- Built as part of Data Warehouse course project
- Implements concepts from database research papers
- Inspired by real-world streaming ETL systems

## 📧 Contact

For questions or collaboration:
- Email: abubakarshahid832@gmail.com
- Project Link: https://github.com/yourusername/walmart-realtime-datawarehouse

---

⭐ **Star this repo if you find it useful!**
