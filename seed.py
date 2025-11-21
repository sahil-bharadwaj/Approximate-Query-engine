"""
Seed script to generate sample data for the AQE database
"""
import sqlite3
import random
import string
from datetime import datetime, timedelta

def generate_sample_data(db_path='aqe.sqlite', num_records=200000):
    """Generate sample purchases data"""
    print(f"🌱 Generating {num_records} sample records...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create purchases table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            country TEXT NOT NULL,
            city TEXT NOT NULL,
            category TEXT NOT NULL,
            purchase_date TEXT NOT NULL
        )
    """)
    
    # Clear existing data
    cursor.execute("DELETE FROM purchases")
    
    # Sample data
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Japan', 'Australia', 'Brazil', 'India', 'China']
    cities = ['New York', 'London', 'Toronto', 'Berlin', 'Paris', 'Tokyo', 'Sydney', 'Rio', 'Mumbai', 'Beijing']
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Home', 'Sports', 'Toys', 'Beauty', 'Garden', 'Automotive']
    
    # Generate data in batches
    batch_size = 1000
    start_date = datetime(2023, 1, 1)
    
    for batch_start in range(0, num_records, batch_size):
        records = []
        for i in range(batch_start, min(batch_start + batch_size, num_records)):
            customer_id = random.randint(1, 50000)
            product_id = random.randint(1, 10000)
            amount = round(random.uniform(10.0, 1000.0), 2)
            country = random.choice(countries)
            city = random.choice(cities)
            category = random.choice(categories)
            days_offset = random.randint(0, 365)
            purchase_date = (start_date + timedelta(days=days_offset)).strftime('%Y-%m-%d')
            
            records.append((
                i + 1,
                customer_id,
                product_id,
                amount,
                country,
                city,
                category,
                purchase_date
            ))
        
        cursor.executemany("""
            INSERT INTO purchases 
            (id, customer_id, product_id, amount, country, city, category, purchase_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, records)
        
        if (batch_start + batch_size) % 10000 == 0:
            print(f"  Generated {batch_start + batch_size} records...")
    
    conn.commit()
    
    # Create indexes
    print("📊 Creating indexes...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_country ON purchases(country)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_category ON purchases(category)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON purchases(purchase_date)")
    
    conn.commit()
    
    # Update statistics
    cursor.execute("SELECT COUNT(*) FROM purchases")
    count = cursor.fetchone()[0]
    
    cursor.execute("""
        INSERT OR REPLACE INTO aqe_table_stats (table_name, row_count, updated_at)
        VALUES ('purchases', ?, CURRENT_TIMESTAMP)
    """, (count,))
    
    conn.commit()
    
    print(f"✅ Successfully generated {count} records")
    print(f"📁 Database: {db_path}")
    
    # Print sample stats
    cursor.execute("SELECT country, COUNT(*) FROM purchases GROUP BY country ORDER BY COUNT(*) DESC LIMIT 5")
    print("\n📈 Top 5 countries by purchases:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")
    
    cursor.execute("SELECT AVG(amount), MIN(amount), MAX(amount) FROM purchases")
    avg, min_amt, max_amt = cursor.fetchone()
    print(f"\n💰 Purchase amounts:")
    print(f"  Average: ${avg:.2f}")
    print(f"  Min: ${min_amt:.2f}")
    print(f"  Max: ${max_amt:.2f}")
    
    conn.close()


if __name__ == '__main__':
    import sys
    
    num_records = 200000
    if len(sys.argv) > 1:
        num_records = int(sys.argv[1])
    
    db_path = 'aqe.sqlite'
    if len(sys.argv) > 2:
        db_path = sys.argv[2]
    
    generate_sample_data(db_path, num_records)
