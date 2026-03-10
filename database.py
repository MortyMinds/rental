import sqlite3
import os

DB_PATH = os.path.join(os.getcwd(), "rental_data.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Unified rentals table
    c.execute('''
        CREATE TABLE IF NOT EXISTS rentals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_id TEXT NOT NULL,
            canonical_url TEXT UNIQUE NOT NULL,
            raw_address TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            beds REAL,
            baths REAL,
            sqft INTEGER,
            property_type TEXT,
            price INTEGER,
            description TEXT,
            extra_metadata TEXT, 
            first_seen DATE DEFAULT CURRENT_DATE,
            last_seen DATE DEFAULT CURRENT_DATE,
            UNIQUE(source, source_id)
        )
    ''')

    # Still keeping raw_snapshots for debugging/re-parsing
    c.execute('''
        CREATE TABLE IF NOT EXISTS raw_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            source_id TEXT,
            raw_data TEXT NOT NULL,
            date DATE DEFAULT CURRENT_DATE
        )
    ''')

    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
    print(f"Unified database initialized at {DB_PATH}")
