
import requests
import time
from urllib.parse import urlparse
from modules.utils import market_utils
from dateutil import parser
from bs4 import BeautifulSoup

# --- CONFIGURATION ---
MARKETAUX_BASE_URL = "https://api.marketaux.com/v1/news/all"

class MarketAuxEngine:
    def __init__(self, api_keys, log_callback):
        self.api_keys = api_keys
        self.log_callback = log_callback
        self.current_key_idx = 0
        self.browser_driver = None
        self.scan_days = 3 # Default scan range

    def _get_next_key(self):
        """Rotates to the next available API key."""
        key = self.api_keys[self.current_key_idx]
        self.current_key_idx = (self.current_key_idx + 1) % len(self.api_keys)
        return key

    def run_company_scan(self, target_date, ticker_list, db=None, cache_map=None, existing_titles=None, headless=False, lookback_start=None, lookback_end=None, trading_session_date=None):
        """
        Main Execution Method (Incremental/Safe Mode).
        1. For each ticker:
           a. Fetch metadata from API (Page 1 & 2)
           b. Scrape content immediately
           c. SAVE TO DB IMMEDIATELY
           d. Return results for UI display
        """
        self.log_callback(f"\n📂 ACTIVATING PHASE: MARKETAUX COMPANY SCANNER (SAFE MODE)")
        self.log_callback(f"├── 🔑 Keys Available: {len(self.api_keys)}")
        
        all_final_reports = []
        # If existing_titles passed, we use it for checking dupes
        if existing_titles is None:
            if db:
                seen_titles = db.fetch_existing_titles(target_date)
            else:
                seen_titles = {}
        else:
            # 🛡️ SAFETY: Convert stale Set cache to Dict
            if isinstance(existing_titles, set):
                seen_titles = {t: "?" for t in existing_titles}
            else:
                seen_titles = existing_titles.copy()
        
        # Initialize Driver once for the session
        self.log_callback(f"├── 🖥️ Launching Scraper (Selenium)...")
        driver = None
        try:
            driver = market_utils.get_selenium_driver(headless=headless)
        except Exception as e:
             self.log_callback(f"❌ Failed to launch Chrome: {e}")
             return []

        # Init Progress Manager
        from modules.utils.scan_progress import ScanProgressManager
        pm = ScanProgressManager()
        
        # 🧠 Smart Tracking Init
        # If no scan is active, or if we are starting a fresh list (heuristic), start tracking.
        # For safety/simplicity: If loop starts, we assume it's a valid run. 
        # We start a NEW tracking session if the first ticker isn't the 'current_target' of an existing one.
        
        curr_state = pm.load_state()
        if not curr_state.get("active_scan"):
             pm.start_new_scan("COMPANY", ticker_list, target_date.strftime("%Y-%m-%d"))
        
        # If we are passed a list that MATCHES the resume list, we continue.
        # If we are passed a DIFFERENT list, we might want to overwrite?
        # For now, let's assume if the user clicked "Start", they want this run tracked.
        # But we need to avoid resetting if we are *resuming* (which calls this same function).
        # We'll rely on app.py passing the *Partial* list for resume.
        # If len(ticker_list) == len(curr_state['total_targets']), it's likely a restart -> overwrite.
        if len(ticker_list) > 0 and len(ticker_list) != len(curr_state.get('remaining_targets', [])):
             # It's a header run? Or a full run.
             # Ideally app.py handles this. But let's implicitly start if not active.
             pass

        try:
            for ticker in ticker_list:
                try:
                    # TRACKING START
                    pm.mark_target_start(ticker)
                    
                    self.log_callback(f"──────────────────────────────────────────────────")
                    self.log_callback(f"🏢 STARTING HUNT: {ticker}")
                    
                    # --- STEP A: MULTI-LAYER DISCOVERY ---
                    
                    # PROTOCOL 1: Google RSS (Free, Fast)
                    layer1_items = self._fetch_google_rss(ticker, target_date, seen_titles, lookback_start=lookback_start, lookback_end=lookback_end)
                    
                    # PROTOCOL 2: MarketAux API (Paid, Backup)
                    layer2_items = self._fetch_ticker_metadata(ticker, target_date)
                    
                    # Merge (L1 first)
                    raw_articles = layer1_items + layer2_items
                    
                    if not raw_articles:
                        self.log_callback(f"│   └── ⚠️ No metadata found for {ticker} (Check 1 & 2 failed)")
                        # Even if no data, we successfully 'scanned' it.
                        pm.mark_target_complete(ticker) 
                        continue
                    else:
                        self.log_callback(f"│   ├── ✅ Targets Acquired: {len(layer1_items)} via RSS, {len(layer2_items)} via API.")

                    # --- STEP B: SCRAPE CONTENT (Per Ticker) ---
                    ticker_reports = []
                    
                    for item in raw_articles:
                        url = item.get("url")
                        title = item.get("title")
                        desc = item.get("description", "")
                        pub_at = item.get("published_at")
                        source = item.get("source")
                        
                        # 1. Title Deduplication
                        # Check exact
                        if title in seen_titles:
                            self.log_callback(f"│   └── ⏭️ Skipping known title: {title[:30]}...")
                            continue
                            
                        # Normalize Check (Robust)
                        norm_title = market_utils.normalize_title(title).lower()
                        
                        if norm_title in seen_titles: 
                            # Audit Trail: Show DB ID
                            db_id = seen_titles[norm_title] if isinstance(seen_titles, dict) else "?"
                            self.log_callback(f"│   └── ⏭️ Skipping: '{title[:40]}...' (Found in DB Row #{db_id})")
                            continue
                        # seen_titles.add(check_title) <-- MOVED: Only add AFTER success

                        # 2. Foreign Title Filter
                        upper_title = title.upper()
                        foreign_markers = ["YAHOO FINANCE UK", "YAHOO! FINANCE CANADA", "YAHOO FINANCE AUSTRALIA", "YAHOO FINANCE SINGAPORE"]
                        is_foreign = False
                        for marker in foreign_markers:
                            if marker in upper_title:
                                 self.log_callback(f"│   └── 🛑 Skipped Foreign Source: '{title[:40]}...' (Detected: {marker})")
                                 is_foreign = True
                                 break
                        if is_foreign: continue
                        
                        # 🚫 FAST BLOCKLIST (Title Scan)
                        # Check if any blocked source is the reason
                        is_blacklisted = False
                        block_reason = ""
                        for bad_src in market_utils.BLOCKED_SOURCES:
                            if bad_src in upper_title:
                                is_blacklisted = True
                                block_reason = bad_src
                                break
                        
                        if is_blacklisted:
                            self.log_callback(f"│   └── 🛑 Skipped: '{title[:40]}...' (Matches Blocklist: {block_reason})")
                            continue
                        
                        # 🛑 STRICT PRE-FLIGHT DOMAIN CHECK
                        try:
                            d_parts = urlparse(url)
                            domain = d_parts.netloc.lower()
                            if "yahoo.com" in domain:
                                if domain not in ["finance.yahoo.com", "www.finance.yahoo.com"]:
                                    self.log_callback(f"│   ├── 🛑 SKIPPING: Non-US Domain detected ({domain})")
                                    continue
                        except Exception:
                            pass
                        
                        
                        # 🚀 URL DEDUP: In-memory check replaces per-article DB query.
                        # INSERT OR IGNORE on UNIQUE url column is the DB-level safety net.
                        clean_url = url.split('?')[0]
                        if cache_map and (url in cache_map or clean_url in cache_map):
                             self.log_callback(f"│   └── ⏭️ Skipping '{title[:30]}...' (URL in session cache)")
                             continue
                            
                        self.log_callback(f"│   ├── 🔹 NEW LEAD: {title[:40]}...")
                        
                        # SMART RETRY LOGIC
                        max_attempts = 1
                        if market_utils.is_premium_source(title, url):
                            max_attempts = 2
                            self.log_callback(f"│   │   └── 🌟 Premium Source Detected. Retries Enabled.")

                        scrape_result = None
                        
                        # 🔄 RETRY LOOP (Start)
                        for attempt in range(max_attempts):
                            try:
                                scrape_result = market_utils.fetch_yahoo_selenium(driver, url, self.log_callback)
                                if scrape_result:
                                    break # Success
                            except market_utils.DeadDriverException:
                                self.log_callback(f"│   │   └── 💀 Browser Died (Attempt {attempt+1}/{max_attempts}). Restarting...")
                                market_utils.force_quit_driver(driver)
                                try:
                                    driver = market_utils.get_selenium_driver()
                                    self.log_callback(f"│   │   └── ♻️ Driver Rebooted Successfully.")
                                except Exception as reboot_err:
                                    self.log_callback(f"│   │   └── ❌ Reboot Failed: {reboot_err}")
                                    break # Stop retrying if we can't get a driver
                            except market_utils.BlockedContentException as be:
                                self.log_callback(f"│   │   └── 🛑 {str(be)}")
                                # SAVE AS BLOCKED TO PREVENT RETRY
                                if db:
                                    # We need parsed time (pub_at is string here, let's parse it or reuse)
                                    try:
                                        dt_obj = parser.parse(pub_at)
                                        iso_time = dt_obj.isoformat()
                                        display_time = dt_obj.strftime("%H:%M %Z%z").strip()
                                    except Exception:
                                        iso_time = pub_at
                                        display_time = "??:??"

                                    blocked_item = {
                                        "title": title,
                                        "url": url, 
                                        "content": ["BLOCKED SOURCE"],
                                        "publisher": "BLOCKED",
                                        "time": display_time,
                                        "published_at": iso_time,
                                        "source_domain": source,
                                        "category": "HIDDEN"
                                    }
                                    db.insert_news([blocked_item], "COMPANY") # Save as COMPANY type but HIDDEN category
                                    
                                    # Update Cache
                                    norm_key = market_utils.normalize_title(title).lower()
                                    seen_titles[norm_key] = "New Session"
                                    self.log_callback(f"│   │   └── 💾 MARKED as BLOCKED in DB.")
                                break # Don't retry blocked content
                            except Exception as e:
                                self.log_callback(f"│   │   └── ⚠️ Fetch Error (Attempt {attempt+1}/{max_attempts}): {e}")
                        # 🔄 RETRY LOOP (End)
                        
                        if scrape_result:
                             if isinstance(scrape_result, list):
                                 content_lines = scrape_result
                                 # Improved Logic: Use existing source if scrape list is raw
                                 publisher = source 
                             else:
                                 content_lines = scrape_result.get("content", [])
                                 # STRICT PRIORITY: Scraped Key > RSS Source > API Source
                                 scraped_pub = scrape_result.get("publisher")
                                 if scraped_pub and scraped_pub != "Yahoo Finance":
                                     publisher = scraped_pub
                                 else:
                                     publisher = source 
                        else:
                             content_lines = [desc] if desc else ["(Content unavailable)"]
                             publisher = source
                        
                        # Parse Time
                        try:
                            dt = parser.parse(pub_at)
                            time_str = dt.strftime("%H:%M %Z%z").strip()
                        except Exception:
                            time_str = "??:??"

                        report = {
                            "title": title,
                            "url": url,
                            "content": content_lines,
                            "publisher": publisher,
                            "time": time_str,
                            "published_at": pub_at,
                            "source_domain": source,
                            "category": ticker
                        }
                        ticker_reports.append(report)
                        all_final_reports.append(report)
                        
                        # Update cache with normalized key to prevent re-fetch in same session
                        norm_key = market_utils.normalize_title(title).lower()
                        seen_titles[norm_key] = "New Session"
                        seen_titles[title] = "New Session" # Also raw logic
                        
                        # 💾 IMMEDIATE SAVE (Per Article)
                        if db:
                            inserted_count, dups_count = db.insert_news([report], "COMPANY", trading_session_date=trading_session_date)
                            if inserted_count > 0:
                                self.log_callback(f"│   │   └── 💾 SAVED to DB immediately.")
                            elif dups_count > 0:
                                self.log_callback(f"│   │   └── ⚠️ Already Exists (Ignored by DB).")
                            else:
                                self.log_callback(f"│   │   └── ⚠️ DB Insert Failed.")
                        
                        self.log_callback(f"│   │   └── 🏆 SECURED.")
                    
                    # TRACKING COMPLETE
                    pm.mark_target_complete(ticker)

                    # 🧹 SANITIZE DRIVER
                    try:
                        if driver: driver.get("about:blank")
                    except Exception: pass
                        
                except market_utils.DeadDriverException:
                    self.log_callback(f"│   └── ❌ CRITICAL: Browser Frozen/Died. Rebooting Driver...")
                    try:
                        if driver: driver.quit()
                    except Exception: pass
                    try:
                        driver = market_utils.get_selenium_driver()
                        self.log_callback(f"│   └── ♻️ Driver Rebooted Successfully.")
                    except Exception as e:
                        self.log_callback(f"│   └── ❌ Reboot Failed: {e}")

                except Exception as e:
                    self.log_callback(f"❌ Critical MarketAux Error: {e}")
        finally:
            if driver:
                try: market_utils.force_quit_driver(driver)
                except Exception: pass
            
            # 🏁 ONLY FINISH if we actually completed the loop
            if 'ticker_list' in locals() and len(ticker_list) > 0:
                 pm.finish_scan()
            
        return all_final_reports


    def _fetch_google_rss(self, ticker, target_date, seen_titles, lookback_start=None, lookback_end=None):
        """
        Fetches Google RSS for a ticker, checking against seen_titles.
        Handles caching and strict date filtering.
        """
        if isinstance(seen_titles, set):
            # 🛡️ SAFETY: Convert stale Set cache to Dict
            seen_titles = {t: "?" for t in seen_titles}
            
        articles = []
        
        # Query: "{ticker} stock news"
        query = f'{ticker} stock news'
        rss_url = f"https://news.google.com/rss/search?q={ticker}+stock+news+when:{self.scan_days}d&hl=en-US&gl=US&ceid=US:en"
        
        try:
            self.log_callback(f"│   ├── 🔎 PROTOCOL 1: Google RSS ('{query}')...")
            resp = requests.get(rss_url, headers=market_utils.HEADERS, timeout=10)
            soup = BeautifulSoup(resp.content, features="xml")
            items = soup.find_all("item")
            
            if not items:
                self.log_callback(f"│   └── ℹ️ Protocol 1 yielded 0 items.")
                return []
                
            self.log_callback(f"│   └── 📥 Found {len(items)} RSS items. Filtering...")
            
            for item in items[:10]: # Limit top 10 relevant
                title = item.title.text
                link = item.link.text
                pub_date_str = item.pubDate.text
                source_str = item.source.text if item.source else "Google News"
                
                # DATE/TIME CHECK (Sliding Window)
                try:
                    pub_dt = parser.parse(pub_date_str)
                    if lookback_start:
                        if pub_dt < lookback_start:
                            continue
                            
                    if lookback_end:
                        if pub_dt > lookback_end:
                            continue
                            
                    if not lookback_start and not lookback_end:
                        if pub_dt.date() != target_date:
                            continue
                except Exception: continue
                
                # Title Dedup
                # Normalization
                norm_title = market_utils.normalize_title(title).lower()
                
                if norm_title in seen_titles: continue
                
                # Check raw title too just in case (if seen_titles has raw keys? No, DB uses norm)
                if title in seen_titles: continue
                
                # Construct Article Object similar to API structure
                articles.append({
                    "title": title,
                    "url": market_utils.decode_google_news_url(link),
                    "description": "(RSS Source) " + title,
                    "published_at": pub_dt.isoformat(), # STORE AS ISO-8601 for SQL Date helper
                    "source": source_str,
                    "is_rss": True # Flag
                })
                
        except Exception as e:
            self.log_callback(f"│   └── ⚠️ RSS Protocol Error: {e}")
            
        return articles

    def _fetch_ticker_metadata(self, ticker, target_date):
        """
        Helper to fetch Page 1 & 2 for a single ticker via MarketAux API.
        """
        articles = []
        ticker_articles_found = 0
        
        for page in [1, 2]:
            date_str = target_date.strftime("%Y-%m-%d")
            
            params = {
                "symbols": ticker,
                "published_on": date_str,
                "language": "en",
                "filter_entities": "true", 
                "limit": 3,
                "page": page 
            }
            
            success = False
            ticker_attempts = 0
            
            while ticker_attempts < 3 and not success:
                current_api_key = self.api_keys[self.current_key_idx]
                params["api_token"] = current_api_key
                
                try:
                    resp = requests.get(MARKETAUX_BASE_URL, params=params, timeout=10)
                    data = resp.json()
                    
                    if "error" in data:
                        err_code = data.get("error", {}).get("code")
                        if err_code == "usage_limit_reached":
                            self.log_callback(f"⚠️ Key exhausted ({current_api_key[:5]}...). Rotating...")
                            self._get_next_key()
                            ticker_attempts += 1
                            continue 
                        else:
                            break 
                    
                    data_items = data.get("data", [])
                    if not data_items:
                         success = True
                         break
                         
                    articles.extend(data_items)
                    ticker_articles_found += len(data_items)
                    success = True
                    
                except Exception as e:
                    self.log_callback(f"❌ API Error: {e}")
                    ticker_attempts += 1
            
            if page == 1 and ticker_articles_found == 0:
                break
            
            time.sleep(0.5)
            
        return articles

def run_marketaux_scan(api_keys, target_date, ticker_list, log_callback, db=None, cache_map=None, existing_titles=None, headless=False, lookback_start=None, lookback_end=None, trading_session_date=None):
    engine = MarketAuxEngine(api_keys, log_callback)
    return engine.run_company_scan(target_date, ticker_list, db, cache_map, existing_titles, headless=headless, lookback_start=lookback_start, lookback_end=lookback_end, trading_session_date=trading_session_date)
