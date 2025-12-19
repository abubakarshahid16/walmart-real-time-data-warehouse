import mysql.connector
import os

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "abubakar123",
    "database": "BlackDW"
}

def run_sql_file(filename, cursor):
    print(f"Executing {filename}...")
    with open(filename, 'r') as f:
        sql = f.read()
    
    # Split by semicolon, but handle cases where it might be inside strings (simplified)
    # For this project, the SQL files seem simple enough to split by semicolon
    commands = sql.split(';')
    for command in commands:
        if command.strip():
            try:
                cursor.execute(command)
            except Exception as e:
                print(f"Error executing command: {e}")

def setup():
    try:
        conn = mysql.connector.connect(host=DB_CONFIG['host'], user=DB_CONFIG['user'], password=DB_CONFIG['password'])
        cursor = conn.cursor()
        
        # Create DB if not exists
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        cursor.execute(f"USE {DB_CONFIG['database']}")
        
        # Check if tables exist
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        if not tables:
            print("No tables found. Running Create-DW.sql...")
            run_sql_file('Create-DW.sql', cursor)
        else:
            print(f"Tables already exist: {tables}")
            
        # Create View from Queries.sql (Q20)
        print("Ensuring STORE_QUARTERLY_SALES view exists...")
        view_sql = """
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
        """
        cursor.execute(view_sql)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Setup complete.")
    except Exception as e:
        print(f"Setup failed: {e}")

if __name__ == "__main__":
    setup()
