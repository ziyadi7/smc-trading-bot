import sqlite3
import json
import datetime as dt
from typing import List, Dict, Optional
from .logging import get_logger

logger = get_logger(__name__)

class SignalStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize database schema"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Signals table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS signals(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_utc TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        tf TEXT NOT NULL,
                        side TEXT NOT NULL,
                        entry REAL NOT NULL,
                        sl REAL NOT NULL,
                        tp1 REAL NOT NULL,
                        tp2 REAL NOT NULL,
                        tp3 REAL NOT NULL,
                        score INTEGER NOT NULL,
                        checklist_json TEXT NOT NULL,
                        notes_json TEXT NOT NULL,
                        ob_tf TEXT NOT NULL,
                        signal_hash TEXT UNIQUE NOT NULL,
                        active BOOLEAN DEFAULT 1,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Outcomes table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS outcomes(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        signal_id INTEGER NOT NULL,
                        hit_type TEXT NOT NULL,
                        price REAL NOT NULL,
                        ts_utc TEXT NOT NULL,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(signal_id) REFERENCES signals(id)
                    )
                """)
                
                # Create indexes
                conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_active ON signals(active, symbol)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_hash ON signals(signal_hash)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_signal ON outcomes(signal_id)")
                
        except Exception as e:
            logger.critical(f"Database initialization failed: {e}")
            raise

    def get_signal_key(self, signal: Dict) -> str:
        """Generate unique hash for signal deduplication"""
        import hashlib
        key_data = f"{signal['symbol']}:{signal['tf']}:{signal['side']}:{signal['entry']:.5f}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def is_duplicate_signal(self, signal_hash: str) -> bool:
        """Check if signal already exists"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT 1 FROM signals WHERE signal_hash = ?",
                    (signal_hash,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Duplicate check failed: {e}")
            return True  # Fail safe

    def save_signal(self, signal: Dict) -> int:
        """Save signal to database"""
        try:
            signal_hash = self.get_signal_key(signal)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    INSERT INTO signals (
                        ts_utc, symbol, tf, side, entry, sl, tp1, tp2, tp3,
                        score, checklist_json, notes_json, ob_tf, signal_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    dt.datetime.utcnow().isoformat(),
                    signal['symbol'],
                    signal['tf'],
                    signal['side'],
                    signal['entry'],
                    signal['sl'],
                    signal['tps'][0], signal['tps'][1], signal['tps'][2],
                    signal['score'],
                    json.dumps(signal['checklist']),
                    json.dumps(signal['notes']),
                    signal['ob'].tf,
                    signal_hash
                ))
                
                signal_id = cursor.lastrowid
                conn.commit()
                logger.info(f"Signal saved with ID: {signal_id}")
                return signal_id
                
        except sqlite3.IntegrityError:
            logger.warning(f"Duplicate signal rejected: {signal_hash}")
            return -1
        except Exception as e:
            logger.error(f"Signal save failed: {e}")
            return -1

    def get_open_signals(self, symbol: str) -> List[Dict]:
        """Get active signals for symbol"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM signals 
                    WHERE active = 1 AND symbol = ?
                    ORDER BY ts_utc DESC
                """, (symbol,))
                
                signals = []
                for row in cursor:
                    signal = dict(row)
                    # Parse JSON fields
                    signal['checklist'] = json.loads(signal['checklist_json'])
                    signal['notes'] = json.loads(signal['notes_json'])
                    signals.append(signal)
                    
                return signals
        except Exception as e:
            logger.error(f"Open signals query failed: {e}")
            return []

    def record_outcome(self, signal_id: int, hit_type: str, price: float):
        """Record TP/SL outcome"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Record outcome
                conn.execute("""
                    INSERT INTO outcomes (signal_id, hit_type, price, ts_utc)
                    VALUES (?, ?, ?, ?)
                """, (signal_id, hit_type, price, dt.datetime.utcnow().isoformat()))
                
                # Deactivate signal if SL or TP3 hit
                if hit_type in ('SL', 'TP3'):
                    conn.execute(
                        "UPDATE signals SET active = 0 WHERE id = ?",
                        (signal_id,)
                    )
                
                conn.commit()
                logger.info(f"Outcome recorded: {signal_id} {hit_type}")
                
        except Exception as e:
            logger.error(f"Outcome recording failed: {e}")

    def is_tp_recorded(self, signal_id: int, tp_level: int) -> bool:
        """Check if TP level was already recorded"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT 1 FROM outcomes WHERE signal_id = ? AND hit_type = ?",
                    (signal_id, f'TP{tp_level}')
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"TP check failed: {e}")
            return True  # Fail safe