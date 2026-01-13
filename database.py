"""
Database module for persistent storage of comparables searches.
Uses SQLite for simplicity.
"""
import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path


class Database:
    """Simple SQLite database for comparables data."""
    
    def __init__(self, db_path: str = "comparables.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS searches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_name TEXT NOT NULL,
                    target_data TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    num_comparables INTEGER NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS comparables (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    search_id INTEGER NOT NULL,
                    rank INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    validation_score REAL NOT NULL,
                    data TEXT NOT NULL,
                    FOREIGN KEY (search_id) REFERENCES searches (id)
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    ticker TEXT,
                    exchange TEXT,
                    is_public BOOLEAN,
                    last_verified DATETIME,
                    verification_data TEXT
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_searches_timestamp 
                ON searches(timestamp DESC)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_comparables_search 
                ON comparables(search_id)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_companies_ticker 
                ON companies(ticker)
            """)
            
            conn.commit()
    
    def save_search(
        self,
        target_name: str,
        target_data: Dict[str, Any],
        comparables: List[Dict[str, Any]],
        metadata: Dict[str, Any]
    ) -> int:
        """
        Save a search and its results.
        
        Returns:
            search_id: ID of the saved search
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Save search
            cursor.execute("""
                INSERT INTO searches (target_name, target_data, metadata, num_comparables)
                VALUES (?, ?, ?, ?)
            """, (
                target_name,
                json.dumps(target_data),
                json.dumps(metadata),
                len(comparables)
            ))
            
            search_id = cursor.lastrowid
            
            # Save comparables
            for rank, comp in enumerate(comparables, 1):
                cursor.execute("""
                    INSERT INTO comparables (search_id, rank, name, ticker, exchange, validation_score, data)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    search_id,
                    rank,
                    comp.get('name', ''),
                    comp.get('ticker', ''),
                    comp.get('exchange', ''),
                    comp.get('validation_score', 0.0),
                    json.dumps(comp)
                ))
                
                # Update companies cache
                self._update_company_cache(
                    comp.get('name', ''),
                    comp.get('ticker', ''),
                    comp.get('exchange', ''),
                    is_public=True,
                    conn=conn
                )
            
            conn.commit()
            return search_id
    
    def get_recent_searches(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent searches."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, target_name, timestamp, num_comparables
                FROM searches
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_search_results(self, search_id: int) -> Optional[Dict[str, Any]]:
        """Get full results for a specific search."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get search info
            cursor.execute("""
                SELECT target_data, metadata
                FROM searches
                WHERE id = ?
            """, (search_id,))
            
            row = cursor.fetchone()
            if not row:
                return None
            
            target_data = json.loads(row['target_data'])
            metadata = json.loads(row['metadata'])
            
            # Get comparables
            cursor.execute("""
                SELECT data
                FROM comparables
                WHERE search_id = ?
                ORDER BY rank
            """, (search_id,))
            
            comparables = [json.loads(row['data']) for row in cursor.fetchall()]
            
            return {
                'target': target_data,
                'comparables': comparables,
                'metadata': metadata
            }
    
    def get_stats(self) -> Dict[str, int]:
        """Get database statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM searches")
            total_searches = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT name) FROM companies")
            unique_companies = cursor.fetchone()[0]
            
            return {
                'total_searches': total_searches,
                'unique_companies': unique_companies
            }
    
    def search_companies(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search for companies in the database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT name, ticker, exchange, is_public, last_verified
                FROM companies
                WHERE name LIKE ? OR ticker LIKE ?
                ORDER BY name
                LIMIT ?
            """, (f"%{query}%", f"%{query}%", limit))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_company_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get cached company information."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT *
                FROM companies
                WHERE ticker = ?
            """, (ticker,))
            
            row = cursor.fetchone()
            if row:
                data = dict(row)
                if data['verification_data']:
                    data['verification_data'] = json.loads(data['verification_data'])
                return data
            return None
    
    def _update_company_cache(
        self,
        name: str,
        ticker: str,
        exchange: str,
        is_public: bool = True,
        verification_data: Optional[Dict[str, Any]] = None,
        conn: Optional[sqlite3.Connection] = None
    ):
        """Update or insert company in cache."""
        should_close = False
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            should_close = True
        
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO companies (name, ticker, exchange, is_public, last_verified, verification_data)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    ticker = excluded.ticker,
                    exchange = excluded.exchange,
                    is_public = excluded.is_public,
                    last_verified = excluded.last_verified,
                    verification_data = excluded.verification_data
            """, (
                name,
                ticker,
                exchange,
                is_public,
                datetime.now().isoformat(),
                json.dumps(verification_data) if verification_data else None
            ))
            
            conn.commit()
            
        finally:
            if should_close:
                conn.close()


class SearchHistory:
    """Helper class for managing search history."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def get_similar_searches(
        self,
        target_name: str,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Find similar previous searches."""
        # Simple implementation - could be enhanced with fuzzy matching
        with sqlite3.connect(self.db.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, target_name, timestamp, num_comparables
                FROM searches
                WHERE target_name LIKE ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (f"%{target_name}%", limit))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_most_common_comparables(
        self,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get companies that appear most frequently as comparables."""
        with sqlite3.connect(self.db.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    name,
                    ticker,
                    exchange,
                    COUNT(*) as frequency,
                    AVG(validation_score) as avg_score
                FROM comparables
                GROUP BY name, ticker, exchange
                ORDER BY frequency DESC, avg_score DESC
                LIMIT ?
            """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
