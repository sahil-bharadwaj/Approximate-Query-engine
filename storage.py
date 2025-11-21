"""
Database storage layer for AQE metadata tables
"""
import sqlite3
from typing import Optional, List, Dict, Any
from datetime import datetime


class Storage:
    """Handles database operations for AQE metadata"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """Establish database connection"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        # Enable Write-Ahead Logging for better concurrency
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        return self.conn
    
    def get_connection(self):
        """Get database connection"""
        if self.conn is None:
            self.connect()
        return self.conn
    
    def ensure_meta_tables(self):
        """Create metadata tables if they don't exist"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Table statistics
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aqe_table_stats (
                table_name TEXT PRIMARY KEY,
                row_count INTEGER DEFAULT 0,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Sample metadata
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aqe_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                sample_table TEXT NOT NULL,
                sample_fraction REAL NOT NULL,
                strata_column TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Sketch metadata
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aqe_sketches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                column_name TEXT,
                sketch_type TEXT NOT NULL,
                sketch_data BLOB NOT NULL,
                parameters TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(table_name, column_name, sketch_type)
            )
        """)
        
        # Strata information
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aqe_strata_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sample_table TEXT NOT NULL,
                strata_key TEXT NOT NULL,
                strata_value TEXT NOT NULL,
                pop_size INTEGER NOT NULL,
                sample_size INTEGER NOT NULL,
                fraction REAL NOT NULL,
                weight REAL NOT NULL,
                variance REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
    
    def upsert_table_row_count(self, table: str, count: int):
        """Update or insert table row count"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO aqe_table_stats(table_name, row_count, updated_at)
            VALUES(?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(table_name) DO UPDATE SET 
                row_count=excluded.row_count, 
                updated_at=CURRENT_TIMESTAMP
        """, (table, count))
        conn.commit()
    
    def insert_sample_meta(self, table: str, sample_table: str, fraction: float):
        """Record sample metadata"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO aqe_samples(table_name, sample_table, sample_fraction, created_at)
            VALUES(?, ?, ?, CURRENT_TIMESTAMP)
        """, (table, sample_table, fraction))
        conn.commit()
    
    def upsert_sketch(self, table: str, column: Optional[str], sketch_type: str, 
                     data: bytes, parameters: str = ""):
        """Store or update a sketch"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO aqe_sketches(table_name, column_name, sketch_type, sketch_data, parameters, created_at)
            VALUES(?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(table_name, column_name, sketch_type) 
            DO UPDATE SET 
                sketch_data=excluded.sketch_data, 
                parameters=excluded.parameters, 
                created_at=CURRENT_TIMESTAMP
        """, (table, column, sketch_type, data, parameters))
        conn.commit()
    
    def get_sketch(self, table: str, column: str, sketch_type: str) -> Optional[Dict[str, Any]]:
        """Retrieve a sketch"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT sketch_data, parameters FROM aqe_sketches 
            WHERE table_name = ? AND column_name = ? AND sketch_type = ?
        """, (table, column, sketch_type))
        row = cursor.fetchone()
        if row:
            return {"sketch_data": row[0], "parameters": row[1]}
        return None
    
    def list_sketches(self, table: str) -> List[Dict[str, Any]]:
        """List all sketches for a table"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name, sketch_type, parameters, 
                   strftime('%s', created_at) as created_at
            FROM aqe_sketches 
            WHERE table_name = ?
            ORDER BY created_at DESC
        """, (table,))
        
        sketches = []
        for row in cursor.fetchall():
            sketches.append({
                "table": table,
                "column": row[0],
                "type": row[1],
                "parameters": row[2] or "{}",
                "created_at": int(row[3]) if row[3] else 0
            })
        return sketches
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
