import libsql_client
import datetime
from dateutil import parser as dt_parser
import modules.utils.market_utils as market_utils

class NewsDatabase:
    def __init__(self, db_url, db_token, init_schema=True):
        # Force HTTPS instead of WSS/LibSQL for stability
        self.url = db_url.replace("wss://", "https://").replace("libsql://", "https://")
        self.token = db_token
        try:
            self.client = libsql_client.create_client_sync(url=self.url, auth_token=db_token)
            if init_schema:
                self._initialize_db()
        except Exception as e:
            print(f"❌ DB Connect Error: {e}")
            self.client = None

    def _initialize_db(self):
        """ Creates tables if they don't exist. Handles Schema Updates. """
        if not self.client: return

        # 1. Check if we need to migrate/rebuild (Add Publisher Column)
        try:
            # Check if table exists and has publisher column
            check_sql = "PRAGMA table_info(market_news)"
            columns = self.client.execute(check_sql).rows
            col_names = [c[1] for c in columns]
            
            if columns and "publisher" not in col_names:
                print("⚠️ Schema Mismatch: Dropping old table to add 'publisher' column...")
                self.client.execute("DROP TABLE IF EXISTS market_news")
        except Exception as e:
            print(f"⚠️ Schema Check Error: {e}")

        # Check if 'country' column exists (Migration)
        try:
            self.client.execute("SELECT country FROM market_calendar LIMIT 1")
        except Exception:
            print("📦 Migrating Schema: Adding 'country' to market_calendar...")
            try:
                self.client.execute("ALTER TABLE market_calendar ADD COLUMN country TEXT")
            except Exception as e:
                print(f"⚠️ Schema Migration (Country) Error: {e}")

        # Check if 'event_time' column exists (Migration)
        try:
            self.client.execute("SELECT event_time FROM market_calendar LIMIT 1")
        except Exception:
            print("📦 Migrating Schema: Adding 'event_time' to market_calendar...")
            try:
                self.client.execute("ALTER TABLE market_calendar ADD COLUMN event_time TEXT")
            except Exception as e:
                print(f"⚠️ Schema Migration (Time) Error: {e}")

        # 2. Create Table (New Schema)
        sql_create = """
        CREATE TABLE IF NOT EXISTS market_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            published_at TEXT,
            title TEXT,
            url TEXT UNIQUE,
            source_domain TEXT,
            publisher TEXT,
            category TEXT,
            content TEXT,
            eps_estimate TEXT,
            eps_reported TEXT,
            eps_surprise TEXT,
            trading_session_date TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            self.client.execute(sql_create)
            # Index on Category/Date for speed
            self.client.execute("CREATE INDEX IF NOT EXISTS idx_cat_date ON market_news(category, published_at);")
            # Index on title for fast dedup lookups (avoids full table scan)
            self.client.execute("CREATE INDEX IF NOT EXISTS idx_title ON market_news(title);")
            # Index on URL for fast existence checks
            self.client.execute("CREATE INDEX IF NOT EXISTS idx_url ON market_news(url);")
        except Exception as e:
            print(f"❌ Schema Init Error: {e}")

        # Migration: Add trading_session_date column if missing
        try:
            self.client.execute("SELECT trading_session_date FROM market_news LIMIT 1")
        except Exception:
            print("📦 Migrating Schema: Adding 'trading_session_date' to market_news...")
            try:
                self.client.execute("ALTER TABLE market_news ADD COLUMN trading_session_date TEXT")
                self.client.execute("CREATE INDEX IF NOT EXISTS idx_session_date ON market_news(trading_session_date);")
            except Exception as e:
                print(f"⚠️ Schema Migration (trading_session_date) Error: {e}")

        # 3. Create Calendar Table
        sql_create_cal = """
        CREATE TABLE IF NOT EXISTS market_calendar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT,
            ticker TEXT,
            event_type TEXT,
            event_date TEXT, -- Stored as ISO/YYYY-MM-DD
            importance TEXT,
            status TEXT DEFAULT 'UPCOMING',
            country TEXT,
            event_time TEXT,
            eps_estimate TEXT,
            eps_reported TEXT,
            eps_surprise TEXT
        );
        """
        try:
             self.client.execute(sql_create_cal)
             self.client.execute("CREATE INDEX IF NOT EXISTS idx_cal_date ON market_calendar(event_date);")
        except Exception as e:
            print(f"❌ Calendar Init Error: {e}")

        # 4. Create Hunt Logs Table (Heartbeat/Observability)
        sql_create_hunts = """
        CREATE TABLE IF NOT EXISTS hunt_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_number INTEGER DEFAULT 1,
            trading_session_date TEXT,
            started_at TEXT,
            ended_at TEXT,
            status TEXT DEFAULT 'RUNNING',
            articles_found INTEGER DEFAULT 0,
            articles_in_session INTEGER DEFAULT 0,
            duration_seconds REAL DEFAULT 0,
            errors TEXT,
            lookback_start TEXT,
            lookback_end TEXT
        );
        """
        try:
            self.client.execute(sql_create_hunts)
            self.client.execute("CREATE INDEX IF NOT EXISTS idx_hunt_date ON hunt_logs(trading_session_date);")
        except Exception as e:
            print(f"❌ Hunt Logs Init Error: {e}")

    def fetch_monitored_tickers(self):
        """
        Fetches the list of tickers from the 'aw_ticker_notes' table (Analyst DB).
        Expected table schema: aw_ticker_notes(ticker, ...)
        """
        if not self.client: return []
        
        sql = "SELECT ticker FROM aw_ticker_notes"
        try:
            rs = self.client.execute(sql)
            # Flatten list of tuples: [('AAPL',), ('TSLA',)] -> ['AAPL', 'TSLA']
            tickers = [row[0] for row in rs.rows if row[0]]
            return sorted(tickers)
        except Exception as e:
            print(f"⚠️ Fetch Tickers Error: {e}")
            return []

    def insert_news(self, news_list, category, trading_session_date=None):
        """
        Inserts a list of news dictionaries into the DB.
        trading_session_date: The logical NYSE trading session this news belongs to (YYYY-MM-DD).
        Returns (inserted_count, duplicate_count)
        """
        if not self.client or not news_list:
            return 0, 0
        
        inserted = 0
        duplicates = 0
        
        session_str = trading_session_date.strftime("%Y-%m-%d") if trading_session_date else None
        
        sql = """
        INSERT OR IGNORE INTO market_news 
        (published_at, title, url, source_domain, publisher, category, content, trading_session_date) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        for item in news_list:
            try:
                # Prepare values
                pub_at = item.get('published_at')
                title = item.get('title')
                url = item.get('url')
                domain = item.get('source_domain', 'Unknown')
                publisher = item.get('publisher', 'Unknown')
                # PREFER ITEM CATEGORY, FALLBACK TO ARG
                item_cat = item.get('category', category)
                content_list = item.get('content', [])
                
                # Convert list content to string if needed
                if isinstance(content_list, list):
                    content_str = "\n".join(content_list)
                else:
                    content_str = str(content_list)

                # Execute
                rs = self.client.execute(sql, [pub_at, title, url, domain, publisher, item_cat, content_str, session_str])
                
                # Check if inserted (rows_affected)
                if rs.rows_affected > 0:
                    inserted += 1
                else:
                    duplicates += 1
                    
            except Exception as e:
                print(f"⚠️ Insert Error for {item.get('title')}: {e}")
        
        # 💾 EXPLICIT COMMIT (Crucial for Persistence)
        try:
            self.client.commit()
        except Exception:
            pass
            
        return inserted, duplicates

    def fetch_news_by_date(self, date_obj, category=None):
        """
        Retrieves news from DB for a specific date.
        If category is None, returns ALL categories.
        Uses range query instead of date() function to leverage indexes.
        """
        if not self.client: return []
        
        # Range query: published_at >= 'YYYY-MM-DD' AND published_at < 'YYYY-MM-DD+1'
        # This allows index usage unlike date(published_at) which forces full table scan
        target_date_str = date_obj.strftime("%Y-%m-%d")
        next_day = date_obj + datetime.timedelta(days=1)
        next_day_str = next_day.strftime("%Y-%m-%d")
        
        if category:
            sql = """
            SELECT title, url, content, published_at, source_domain, category, publisher
            FROM market_news 
            WHERE category = ? 
            AND published_at >= ? AND published_at < ?
            AND category != 'HIDDEN'
            AND publisher != 'BLOCKED'
            ORDER BY published_at DESC
            """
            params = [category, target_date_str, next_day_str]
        else:
            sql = """
            SELECT title, url, content, published_at, source_domain, category, publisher
            FROM market_news 
            WHERE published_at >= ? AND published_at < ?
            AND category != 'HIDDEN'
            AND publisher != 'BLOCKED'
            ORDER BY published_at DESC
            """
            params = [target_date_str, next_day_str]
        
        try:
            rs = self.client.execute(sql, params)
            results = []
            for row in rs.rows:
                content_str = row[2]
                content_list = content_str.split("\n") if content_str else []
                
                try:
                    dt = dt_parser.parse(row[3])
                    dt_utc = dt.astimezone(datetime.timezone.utc)
                    time_str = dt_utc.strftime("%H:%M UTC").strip()
                except Exception:
                    time_str = "??:??"

                publisher_val = row[6] if len(row) > 6 and row[6] else "Unknown"

                results.append({
                    "title": row[0],
                    "url": row[1],
                    "content": content_list,
                    "time": time_str,
                    "published_at": row[3],
                    "source_domain": row[4],
                    "category": row[5],
                    "publisher": publisher_val
                })
            return results
        except Exception as e:
            print(f"⚠️ Fetch Error: {e}")
            return []

    def fetch_cache_map(self, date_obj, category=None):
        """
        Returns a lightweight dict of {url: True} for the given date.
        Used for fast URL deduplication without loading full article content.
        """
        if not self.client: return {}
        
        target_date_str = date_obj.strftime("%Y-%m-%d")
        next_day = date_obj + datetime.timedelta(days=1)
        next_day_str = next_day.strftime("%Y-%m-%d")
        
        if category:
            sql = "SELECT url FROM market_news WHERE category = ? AND published_at >= ? AND published_at < ?"
            params = [category, target_date_str, next_day_str]
        else:
            sql = "SELECT url FROM market_news WHERE published_at >= ? AND published_at < ?"
            params = [target_date_str, next_day_str]
        
        try:
            rs = self.client.execute(sql, params)
            return {row[0]: True for row in rs.rows if row[0]}
        except Exception as e:
            print(f"⚠️ Fetch Cache Map Error: {e}")
            return {}

    def fetch_existing_titles(self, date_obj):
        """ Returns a DICT of {normalized_title: id} for the given date for fast deduplication. """
        if not self.client: return {}
        # Range query instead of LIKE for index usage
        target_date_str = date_obj.strftime("%Y-%m-%d")
        next_day = date_obj + datetime.timedelta(days=1)
        next_day_str = next_day.strftime("%Y-%m-%d")
        try:
            sql = "SELECT id, title FROM market_news WHERE published_at >= ? AND published_at < ?"
            rs = self.client.execute(sql, [target_date_str, next_day_str])
            titles_map = {}
            for row in rs.rows:
                norm_t = market_utils.normalize_title(row[1]).lower()
                titles_map[norm_t] = row[0]
            return titles_map
        except Exception as e:
            print(f"⚠️ Fetch Existing Titles Error: {e}")
            return {}

    def fetch_existing_titles_range(self, start_iso, end_iso):
        """
        Range-based deduplication: Returns {normalized_title: id} for all articles
        whose published_at falls between start_iso and end_iso.
        This is critical for weekend/holiday leaps where a session spans multiple calendar days.
        """
        if not self.client: return {}
        try:
            sql = "SELECT id, title FROM market_news WHERE published_at >= ? AND published_at <= ?"
            rs = self.client.execute(sql, [start_iso, end_iso])
            titles_map = {}
            for row in rs.rows:
                norm_t = market_utils.normalize_title(row[1]).lower()
                titles_map[norm_t] = row[0]
            return titles_map
        except Exception as e:
            print(f"⚠️ Fetch Existing Titles (Range) Error: {e}")
            return {}

    def fetch_recent_news(self, limit=50):
        """
        Retrieves the latest news across ALL categories.
        """
        if not self.client: return []
        
        sql = """
        SELECT title, url, content, published_at, source_domain, category, publisher
        FROM market_news 
        ORDER BY published_at DESC
        LIMIT ?
        """
        
        try:
            rs = self.client.execute(sql, [limit])
            results = []
            for row in rs.rows:
                content_str = row[2]
                content_list = content_str.split("\n") if content_str else []
                
                try:
                    # Robust parsing
                    dt = dt_parser.parse(row[3])
                    
                    # Convert to UTC
                    dt_utc = dt.astimezone(datetime.timezone.utc)
                    time_str = dt_utc.strftime("%H:%M %d-%b UTC")
                except Exception:
                    time_str = "Unknown"

                # row indices: 0=title, 1=url, 2=content, 3=pub, 4=src, 5=cat, 6=publisher
                pub_name = row[6] if len(row) > 6 else "Unknown"

                results.append({
                    "title": row[0],
                    "url": row[1],
                    "content": content_list,
                    "time": time_str,
                    "published_at": row[3],
                    "source_domain": row[4],
                    "category": row[5],
                    "publisher": pub_name
                })
            return results
        except Exception as e:
            print(f"⚠️ Fetch Recent Error: {e}")
            return []

    def fetch_news_range(self, start_iso, end_iso):
        """
        Retrieves news strictly between start_iso and end_iso.
        """
        if not self.client: return []
        
        sql = """
        SELECT title, url, content, published_at, source_domain, category, publisher
        FROM market_news 
        WHERE published_at >= ? AND published_at <= ?
        ORDER BY published_at DESC
        """
        
        try:
            rs = self.client.execute(sql, [start_iso, end_iso])
            results = []
            for row in rs.rows:
                content_str = row[2]
                content_list = content_str.split("\n") if content_str else []
                
                try:
                    # Robust Parsing
                    dt = dt_parser.parse(row[3])
                    
                    # Convert to UTC
                    dt_utc = dt.astimezone(datetime.timezone.utc)
                    time_str = dt_utc.strftime("%H:%M %d-%b UTC")
                except Exception:
                    time_str = "Unknown"

                pub_name = row[6] if len(row) > 6 else "Unknown"

                results.append({
                    "title": row[0],
                    "url": row[1],
                    "content": content_list,
                    "time": time_str,
                    "published_at": row[3],
                    "source_domain": row[4],
                    "category": row[5],
                    "publisher": pub_name
                })
            return results
        except Exception as e:
            print(f"⚠️ Fetch Range Error: {e}")
            return []

    def count_news_range(self, start_iso, end_iso):
        """
        Efficiently counts news items between two ISO timestamps.
        """
        if not self.client: return 0
        sql = "SELECT COUNT(id) FROM market_news WHERE published_at >= ? AND published_at <= ?"
        try:
            rs = self.client.execute(sql, [start_iso, end_iso])
            if rs.rows:
                return rs.rows[0][0]
            return 0
        except Exception as e:
            print(f"⚠️ Count Range Error: {e}")
            return 0

    # --- CALENDAR METHODS ---
    def clear_calendar(self):
        """ Clears all upcoming events to allow a fresh sync. """
        if not self.client: return
        try:
            self.client.execute("DELETE FROM market_calendar")
        except Exception as e:
            print(f"⚠️ Clear Calendar Error: {e}")

    def insert_calendar_events(self, events_list):
        """
        Inserts a list of event dicts:
        { "name": "CPI", "ticker": None, "type": "MACRO", "date": "...", "importance": "HIGH" }
        """
        if not self.client or not events_list: return 0
        
        sql = "INSERT INTO market_calendar (event_name, ticker, event_type, event_date, importance, country, event_time, eps_estimate, eps_reported, eps_surprise) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        
        stmts = []
        for ev in events_list:
            try:
                params = [
                    ev['name'], 
                    ev.get('ticker'), 
                    ev['type'], 
                    ev['date'], 
                    ev.get('importance', 'MEDIUM'), 
                    ev.get('country', 'US'),
                    ev.get('time', 'TBA'),
                    ev.get('eps_estimate', '-'),
                    ev.get('eps_reported', '-'),
                    ev.get('eps_surprise', '-')
                ]
                stmts.append(libsql_client.Statement(sql, params))
            except Exception as e:
                print(f"⚠️ Prepare Event Error: {e}")

        if not stmts: return 0

        try:
            # 🚀 Batch Execute (Single Transaction)
            rs = self.client.batch(stmts)
            return len(stmts)
        except Exception as e:
            print(f"⚠️ Batch Insert Error: {e}")
            return 0

    def get_upcoming_events(self, start_date_iso, end_date_iso):
        """ Fetches events between two dates (inclusive) """
        if not self.client: return []
        sql = "SELECT event_name, ticker, event_type, event_date, importance, country, event_time, eps_estimate, eps_reported, eps_surprise FROM market_calendar WHERE event_date >= ? AND event_date <= ? ORDER BY event_date ASC"
        try:
            rs = self.client.execute(sql, [start_date_iso, end_date_iso])
            events = []
            for row in rs.rows:
                events.append({
                    "name": row[0],
                    "ticker": row[1],
                    "type": row[2],
                    "date": row[3],
                    "importance": row[4],
                    "country": row[5] if len(row) > 5 else "US",
                    "time": row[6] if len(row) > 6 else "TBA",
                    "eps_estimate": row[7] if len(row) > 7 else "-",
                    "eps_reported": row[8] if len(row) > 8 else "-",
                    "eps_surprise": row[9] if len(row) > 9 else "-"
                })
            return events
        except Exception as e:
            print(f"⚠️ Get Events Error: {e}")
            return []

    def article_exists(self, url, title=None):
        """
        Checks if an article exists by URL (PRIMARY) or Title (FALLBACK).
        Uses a single combined query with OR to minimize round-trips.
        Both url and title columns are now indexed.
        """
        if not self.client: return False
        
        try:
            if title:
                # Single query: check URL OR Title in one round-trip
                sql = "SELECT id FROM market_news WHERE url = ? OR title = ? LIMIT 1"
                rs = self.client.execute(sql, [url, title])
            else:
                sql = "SELECT id FROM market_news WHERE url = ? LIMIT 1"
                rs = self.client.execute(sql, [url])
            
            if rs.rows:
                return rs.rows[0][0]
            return False
        except Exception as e:
            print(f"⚠️ Existence Check Error: {e}")
            return False

    def batch_urls_exist(self, urls):
        """
        Bulk check: Returns a set of URLs that already exist in the database.
        Much more efficient than checking one-by-one.
        """
        if not self.client or not urls: return set()
        
        existing = set()
        try:
            # Process in batches of 50 to avoid query size limits
            url_list = list(urls)
            for i in range(0, len(url_list), 50):
                batch = url_list[i:i+50]
                placeholders = ','.join(['?' for _ in batch])
                sql = f"SELECT url FROM market_news WHERE url IN ({placeholders})"
                rs = self.client.execute(sql, batch)
                for row in rs.rows:
                    existing.add(row[0])
            return existing
        except Exception as e:
            print(f"⚠️ Batch URL Check Error: {e}")
            return set()

    def get_last_update_time(self):
        """ Returns the timestamp of the most recently added news item. """
        if not self.client: return None
        try:
            rs = self.client.execute("SELECT MAX(created_at) FROM market_news")
            if rs.rows and rs.rows[0][0]:
                return rs.rows[0][0]
            return None
        except Exception as e:
            print(f"⚠️ Failed to fetch last update time: {e}")
            return None

    # --- HUNT HEARTBEAT METHODS ---
    def log_hunt_start(self, run_number, trading_session_date, lookback_start, lookback_end):
        """
        Records the start of a Hunt run. Returns the hunt_log ID for later update.
        """
        if not self.client: return None
        started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        session_str = trading_session_date.strftime("%Y-%m-%d") if hasattr(trading_session_date, 'strftime') else str(trading_session_date)
        lb_start = lookback_start.isoformat() if hasattr(lookback_start, 'isoformat') else str(lookback_start)
        lb_end = lookback_end.isoformat() if hasattr(lookback_end, 'isoformat') else str(lookback_end)
        try:
            sql = """INSERT INTO hunt_logs 
                     (run_number, trading_session_date, started_at, status, lookback_start, lookback_end) 
                     VALUES (?, ?, ?, 'RUNNING', ?, ?)"""
            rs = self.client.execute(sql, [run_number, session_str, started_at, lb_start, lb_end])
            # Get the last inserted row ID
            id_rs = self.client.execute("SELECT last_insert_rowid()")
            if id_rs.rows:
                return id_rs.rows[0][0]
            return None
        except Exception as e:
            print(f"⚠️ Hunt Log Start Error: {e}")
            return None

    def log_hunt_end(self, hunt_id, status, articles_found, articles_in_session, duration_seconds, errors=None):
        """
        Updates a Hunt log entry with the final results.
        status: 'SUCCESS', 'PARTIAL', or 'FAILED'
        """
        if not self.client or not hunt_id: return
        ended_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        errors_str = "; ".join(errors) if errors else None
        try:
            sql = """UPDATE hunt_logs 
                     SET ended_at = ?, status = ?, articles_found = ?, 
                         articles_in_session = ?, duration_seconds = ?, errors = ?
                     WHERE id = ?"""
            self.client.execute(sql, [ended_at, status, articles_found, articles_in_session, duration_seconds, errors_str, hunt_id])
        except Exception as e:
            print(f"⚠️ Hunt Log End Error: {e}")

    def close(self):
        """ Properly closes the database client. """
        if self.client:
            try:
                self.client.close()
                self.client = None
            except Exception:
                pass
