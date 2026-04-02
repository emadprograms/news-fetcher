import requests
from bs4 import BeautifulSoup
import time
from dateutil import parser
from urllib.parse import urlparse
from modules.utils import market_utils
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIGURATION (Yahoo Stocks - Grandmaster Edition) ---
YAHOO_RSS_TARGETS = [
    # 1. EARNINGS (The Numbers)
    {
        "name": "Earnings & Results",
        "category": "EARNINGS",
        "rss_url": "https://news.google.com/rss/search?q=intitle:Earnings+OR+intitle:Revenue+OR+intitle:EPS+OR+intitle:Results+site:finance.yahoo.com&hl=en-US&gl=US&ceid=US:en"
    },
    # 2. ANALYST_RATINGS (Broker Movers)
    {
        "name": "Analyst Upgrades/Downgrades",
        "category": "ANALYST_RATINGS",
        "rss_url": "https://news.google.com/rss/search?q=intitle:Upgrade+OR+intitle:Downgrade+OR+intitle:%22Price+Target%22+OR+intitle:Overweight+OR+intitle:Underweight+site:finance.yahoo.com&hl=en-US&gl=US&ceid=US:en"
    },
    # 3. M&A (Deals)
    {
        "name": "Mergers & Acquisitions",
        "category": "MERGERS_ACQUISITIONS",
        "rss_url": "https://news.google.com/rss/search?q=intitle:Acquisition+OR+intitle:Merger+OR+intitle:Buyout+OR+intitle:%22To+Buy%22+OR+intitle:Deal+site:finance.yahoo.com&hl=en-US&gl=US&ceid=US:en"
    },
    # 4. IPO (New Listings)
    {
        "name": "IPO & Public Offerings",
        "category": "IPO",
        "rss_url": "https://news.google.com/rss/search?q=intitle:IPO+OR+intitle:%22Public+Offering%22+site:finance.yahoo.com&hl=en-US&gl=US&ceid=US:en"
    },
    # 5. INSIDER (Smart Money)
    {
        "name": "Insider Trades & Buybacks",
        "category": "INSIDER_MOVES",
        "rss_url": "https://news.google.com/rss/search?q=intitle:%22Insider+Trading%22+OR+intitle:Buyback+site:finance.yahoo.com&hl=en-US&gl=US&ceid=US:en"
    },
    # 6. SECTOR (Broad View)
    {
        "name": "Sector News (Tech/Energy/Banks)",
        "category": "SECTOR_NEWS",
        "rss_url": "https://news.google.com/rss/search?q=intitle:Tech+OR+intitle:Energy+OR+intitle:Banks+site:finance.yahoo.com&hl=en-US&gl=US&ceid=US:en"
    },
    # 7. EQUITIES (General)
    {
        "name": "General Stock Market",
        "category": "EQUITIES",
        "rss_url": "https://news.google.com/rss/search?q=%22Stock+Market%22+site:finance.yahoo.com&hl=en-US&gl=US&ceid=US:en"
    }
]



def run_stocks_scan(target_date, max_pages, log_callback, db=None, cache_map=None, existing_titles=None, resume_targets=None, target_subset=None, headless=False, lookback_start=None, lookback_end=None, trading_session_date=None):
    """
    Main entry point for Stocks Scan.
    scans YAHOO_RSS_TARGETS for news.
    Returns dict: {"articles": [...], "errors": [...]}
    """
    found_reports = []
    scan_errors = []  # Track errors for Discord reporting
    consecutive_driver_failures = 0
    MAX_CONSECUTIVE_FAILURES = 3
    seen_titles = {}
    if existing_titles:
        if isinstance(existing_titles, set):
            seen_titles = {t: "?" for t in existing_titles} # Convert for robust check
        else:
            seen_titles = existing_titles.copy() # Pre-load confirmed titles
            
    # Init seen_urls from cache_map if available
    if cache_map:
        seen_urls = set(cache_map.keys())
    else:
        seen_urls = set()
    
    log_callback(f"\n📂 ACTIVATING PHASE: YAHOO STOCKS SCANNER (SELENIUM)")
    if target_subset:
        log_callback(f"├── 🎯 Filtered Mode: Scanning {len(target_subset)} specific sectors.")
        
    log_callback(f"├── 🖥️ Launching Chrome Driver (This may take a moment)...")
    
    try:
        driver = market_utils.get_selenium_driver(headless=headless)
    except Exception as e:
        err_msg = f"Failed to launch Chrome: {str(e)}"
        log_callback(f"❌ {err_msg}")
        log_callback(f"   (Run 'pip install selenium webdriver-manager')")
        return {"articles": [], "errors": [err_msg]}

    item_limit = max_pages * 20
    
    try:
        active_targets = YAHOO_RSS_TARGETS
        if target_subset:
            subset_set = set(target_subset)
            active_targets = [t for t in YAHOO_RSS_TARGETS if t['name'] in subset_set]

        # Init Progress Manager
        from modules.utils.scan_progress import ScanProgressManager
        pm = ScanProgressManager()
        
        # 🧠 Smart Tracking Init (Heuristic)
        target_names = [t['name'] for t in active_targets]
        curr_state = pm.load_state()
        if not curr_state.get("active_scan"):
             pm.start_new_scan("STOCKS", target_names, target_date.strftime("%Y-%m-%d"))
        
        # FILTER IF RESUMING
        if resume_targets:
            resume_set = set(resume_targets)
            active_targets = [t for t in active_targets if t['name'] in resume_set]
            log_callback(f"♻️ RESUMING SCAN: {len(active_targets)} Categories Remaining.")

        for target in active_targets:
            feed_name = target['name']
            
            # TRACKING START
            pm.mark_target_start(feed_name)
            
            category_tag = target['category']
            rss_url = target['rss_url']
            
            log_callback(f"├── 📡 Connecting to Feed: {feed_name}...")
            
            try:
                # Use standard requests for RSS (Faster)
                resp = requests.get(rss_url, headers=market_utils.HEADERS, timeout=10)
                soup = BeautifulSoup(resp.content, features="xml")
                items = soup.find_all("item")
                
                if not items:
                    log_callback(f"│   └── ⚠️ Feed empty. Moving to next...")
                    continue
                    
                log_callback(f"│   ├── 📥 Received {len(items)} entries from RSS stream.")
                
                processed_count = 0
                for item in items:
                    if processed_count >= item_limit:
                        log_callback(f"│   └── 🛑 Depth limit reached ({item_limit} items).")
                        break
                    
                    processed_count += 1
                    title = item.title.text
                    pub_date_str = item.pubDate.text
                    google_link = item.link.text
                    
                    try:
                        # Parse full datetime including time
                        pub_dt = parser.parse(pub_date_str)
                        pub_date_only = pub_dt.date()
                    except:
                        continue 
                    
                    # DATE/TIME CHECK (Sliding Window)
                    if lookback_start:
                        if pub_dt < lookback_start:
                            continue
                            
                    if lookback_end:
                        if pub_dt > lookback_end:
                            continue
                            
                    if not lookback_start and not lookback_end:
                        # Fallback to strict date check if no lookback provided
                        if pub_date_only != target_date:
                            continue
                    
                    if title in seen_titles: 
                        log_callback(f"│   └── ⏭️ Skipping known title: {title[:30]}...") 
                        continue
                    
                    # Normalize Check
                    norm_title = market_utils.normalize_title(title).lower()

                    if norm_title in seen_titles:
                         # Audit Trail: Show DB ID
                         db_id = seen_titles[norm_title] if isinstance(seen_titles, dict) else "?"
                         log_callback(f"│   └── ⏭️ Skipping: '{title[:30]}...' (Found in DB Row #{db_id})")
                         continue

                    # 🚫 FAST TITLE BLOCKLIST (Catch them before URL resolve)
                    t_low = title.lower()
                    if "motley fool" in t_low or "zacks" in t_low or "benzinga" in t_low:
                         log_callback(f"│   └── 🛑 SKIPPING: Blocked Keyword in Title.")
                         # SAVE AS HIDDEN
                         if db:
                            blocked_item = {
                                "title": title,
                                "url": market_utils.decode_google_news_url(google_link), # Decode needed for unique URL
                                "content": ["BLOCKED TITLE KEYWORD"],
                                "publisher": "BLOCKED",
                                "time": pub_dt.strftime("%H:%M %Z%z").strip(),
                                "published_at": pub_dt.isoformat(),
                                "source_domain": "finance.yahoo.com",
                                "category": "HIDDEN"
                            }
                            db.insert_news([blocked_item], "HIDDEN")
                            seen_titles[norm_title] = "New Session"
                         continue

                    # seen_titles.add(check_title) <-- MOVED to post-success
                    
                    # Show Time in Log
                    log_callback(f"│   ├── 🔹 CANDIDATE DETECTED: '{title}' [{category_tag}]")
                    log_callback(f"│   │   ├── 🕒 Time: {pub_dt.strftime('%Y-%m-%d %H:%M %Z%z').strip()} (Verified via RSS)")
                    
                    real_url = market_utils.decode_google_news_url(google_link)
                    clean_url = real_url.split('?')[0]

                    # 🚀 URL DEDUP: In-memory check replaces per-article DB query.
                    # INSERT OR IGNORE on UNIQUE url column is the DB-level safety net.
                    if clean_url in seen_urls or real_url in seen_urls:
                         log_callback(f"│   └── ⏭️ Skipping '{title[:30]}...' (URL in session cache)")
                         continue

                    # 🛑 STRICT PRE-FLIGHT DOMAIN CHECK
                    try:
                        d_parts = urlparse(clean_url)
                        domain = d_parts.netloc.lower()
                        if "yahoo.com" in domain:
                            if domain not in ["finance.yahoo.com", "www.finance.yahoo.com"]:
                                log_callback(f"│   │   └── 🛑 SKIPPING: Non-US Domain detected ({domain})")
                                continue
                    except:
                        pass
                        
                    # 🚫 FAST URL BLOCKLIST (Avoid Loading Garbage)
                    u_low = clean_url.lower()
                    if "motley-fool" in u_low or "zacks" in u_low or "benzinga" in u_low:
                         log_callback(f"│   │   └── 🛑 SKIPPING: Blocked Keyword in URL.")
                         
                         # SAVE AS HIDDEN (To Skip Title Next Time)
                         if db:
                            blocked_item = {
                                "title": title,
                                "url": real_url,
                                "content": ["BLOCKED URL KEYWORD"],
                                "publisher": "BLOCKED",
                                "time": pub_dt.strftime("%H:%M %Z%z").strip(),
                                "published_at": pub_dt.isoformat(),
                                "source_domain": "finance.yahoo.com",
                                "category": "HIDDEN"
                            }
                            db.insert_news([blocked_item], "HIDDEN")
                            # Update Cache
                            norm_key = market_utils.normalize_title(title).lower()
                            seen_titles[norm_key] = "New Session"
                            
                         continue


                    if clean_url in seen_urls: 
                         log_callback(f"│   └── ⏭️ Skipping duplicate URL (Session Cache).")
                         continue
                    # seen_urls.add(clean_url) <-- MOVED: Only add AFTER success
                    
                    url_valid = False
                    content = None
                    
                    # SMART RETRY LOGIC
                    max_attempts = 1
                    if market_utils.is_premium_source(title, real_url):
                        max_attempts = 2
                        log_callback(f"│   │   └── 🌟 Premium Source Detected. Retries Enabled.")

                    # 🩺 PRE-FETCH HEALTH CHECK
                    if not market_utils.is_driver_alive(driver):
                        log_callback(f"│   │   └── 💀 Driver dead before fetch. Rebooting...")
                        market_utils.force_quit_driver(driver)
                        try:
                            driver = market_utils.get_selenium_driver(headless=headless)
                            log_callback(f"│   │   └── ♻️ Driver Rebooted Successfully.")
                            consecutive_driver_failures = 0
                        except Exception as reboot_err:
                            consecutive_driver_failures += 1
                            log_callback(f"│   │   └── ❌ Reboot Failed ({consecutive_driver_failures}/{MAX_CONSECUTIVE_FAILURES}): {reboot_err}")
                            if consecutive_driver_failures >= MAX_CONSECUTIVE_FAILURES:
                                err_msg = f"Stocks scan aborted: Chrome reboot failed {MAX_CONSECUTIVE_FAILURES} times consecutively"
                                log_callback(f"│   │   └── 🚨 {err_msg}")
                                scan_errors.append(err_msg)
                                break
                            continue

                    # 🔄 RETRY LOOP (Start)
                    for attempt in range(max_attempts):
                        try:
                            # DYNAMIC ALLOW LIST

                            content = market_utils.fetch_yahoo_selenium(driver, real_url, log_callback)
                            if content:
                                url_valid = True
                                consecutive_driver_failures = 0  # Reset on success
                                break # Success!
                        except market_utils.DeadDriverException:
                            log_callback(f"│   │   └── 💀 Browser Died (Attempt {attempt+1}/{max_attempts}). Restarting...")
                            market_utils.force_quit_driver(driver)
                            try:
                                driver = market_utils.get_selenium_driver(headless=headless)
                                consecutive_driver_failures = 0
                            except Exception as reboot_err:
                                consecutive_driver_failures += 1
                                if consecutive_driver_failures >= MAX_CONSECUTIVE_FAILURES:
                                    err_msg = f"Stocks scan aborted: Chrome reboot failed {MAX_CONSECUTIVE_FAILURES} times"
                                    scan_errors.append(err_msg)
                                break
                            # Loop continues to next attempt
                        except market_utils.BlockedContentException as be:
                            log_callback(f"│   │   └── 🛑 {str(be)}")
                            # SAVE AS BLOCKED TO PREVENT RETRY
                            if db:
                                blocked_item = {
                                    "title": title,
                                    "url": real_url,
                                    "content": ["BLOCKED SOURCE"],
                                    "publisher": "BLOCKED",
                                    "time": pub_dt.strftime("%H:%M %Z%z").strip(),
                                    "published_at": pub_dt.isoformat(),
                                    "source_domain": "finance.yahoo.com",
                                    "category": "HIDDEN"
                                }
                                db.insert_news([blocked_item], "HIDDEN")
                                
                                # Update Cache
                                norm_key = market_utils.normalize_title(title).lower()
                                seen_titles[norm_key] = "New Session"
                                log_callback(f"│   │   └── 💾 MARKED as BLOCKED in DB.")
                            break # abort retries
                        except Exception as e:
                            log_callback(f"│   │   └── ⚠️ Fetch Error (Attempt {attempt+1}/3): {e}")
                            # Loop continues
                    # 🔄 RETRY LOOP (End)

                    if url_valid and content:
                        # 🛡️ REDUNDANT FINAL CHECK (Paranoid Mode)
                        try:
                            final_u = driver.current_url
                            f_p = urlparse(final_u)
                            f_d = f_p.netloc.lower()
                            if "yahoo.com" in f_d and f_d not in ["finance.yahoo.com", "www.finance.yahoo.com"]:
                                log_callback(f"│   │   └── 🛑 FINAL SECURITY BLOCK: Discarding non-US content from {f_d}")
                                continue
                        except:
                            pass

                        # UNPACK DICT (Handle Legacy List Return for safety)
                        if isinstance(content, list):
                            content_list = content
                            publisher = "Yahoo Finance" 
                        else:
                            content_list = content.get("content", [])
                            publisher = content.get("publisher", "Yahoo Finance")

                        report_item = {
                            "title": title,
                            "url": real_url,
                            "content": content_list,
                            "publisher": publisher,
                            "time": pub_dt.strftime("%H:%M %Z%z").strip(), # Store time for UI
                            "published_at": pub_dt.isoformat(), # Store raw time for DB
                            "source_domain": "finance.yahoo.com",
                            "category": category_tag
                        }
                        
                        # 💾 INCREMENTAL SAVE
                        if db:
                            inserted_count, dups_count = db.insert_news([report_item], category_tag, trading_session_date=trading_session_date)
                            if inserted_count > 0:
                                log_callback(f"│   │   └── 💾 SAVED to DB immediately.")
                            elif dups_count > 0:
                                log_callback(f"│   │   └── ⚠️ Already Exists (Ignored by DB).")
                            else:
                                log_callback(f"│   │   └── ⚠️ DB Insert Failed.")
                            
                        found_reports.append(report_item)
                        seen_urls.add(clean_url)
                        
                        # Update cache with normalized key
                        norm_key = market_utils.normalize_title(title).lower()
                        seen_titles[norm_key] = "New Session"
                        seen_titles[title] = "New Session"
                        log_callback(f"│   │   └── 🏆 REPORT SECURED! [Source: {publisher}]")
                    else:
                         # 🛑 MANUAL FALLBACK SECURITY CHECK
                        # Before we save this as "Manual Read", assume the scraper MIGHT have blocked it.
                        # Check where the driver ended up.
                        try:
                            final_url = driver.current_url
                            f_parsed = urlparse(final_url)
                            f_domain = f_parsed.netloc.lower()
                            
                            is_blocked = False
                            if "yahoo.com" in f_domain:
                                if f_domain not in ["finance.yahoo.com", "www.finance.yahoo.com"]:
                                    is_blocked = True
                            
                            if is_blocked:
                                log_callback(f"│   │   └── 🛑 FINAL BLOCK: Non-US Domain ({f_domain}). Discarding.")
                            else:
                                # Safe to save as Manual Read
                                found_reports.append({
                                    "title": f"🔗 [MANUAL READ] {title}",
                                    "url": final_url, # Use final resolved URL
                                    "content": [
                                        "⚠️ Automated extraction failed (Video/Protected Content).",
                                        f"Please click the link above to read manually on Yahoo Finance."
                                    ],
                                    "time": pub_dt.strftime("%H:%M %Z%z").strip(),
                                    "published_at": pub_dt.isoformat(),
                                    "source_domain": "finance.yahoo.com",
                                    "category": category_tag
                                })
                                log_callback(f"│   │   └── ⚠️ Saved for Manual Reading.")
                        except:
                             log_callback(f"│   │   └── ⚠️ Driver error during check. Skipping.")

                    log_callback(f"──────────────────────────────────────────────────")

                    # 🧹 SANITIZE DRIVER
                    try: 
                        if driver: driver.get("about:blank")
                    except:
                        # Driver is dead — force reboot now
                        log_callback(f"│   │   └── 💀 Driver died during sanitization. Rebooting...")
                        market_utils.force_quit_driver(driver)
                        try:
                            driver = market_utils.get_selenium_driver(headless=headless)
                            consecutive_driver_failures = 0
                        except Exception as reboot_err:
                            consecutive_driver_failures += 1
                            if consecutive_driver_failures >= MAX_CONSECUTIVE_FAILURES:
                                err_msg = f"Stocks scan aborted: Chrome reboot failed {MAX_CONSECUTIVE_FAILURES} times"
                                scan_errors.append(err_msg)
                                break
                    
            except Exception as e:
                log_callback(f"│   └── ❌ RSS Error: {str(e)}")
                scan_errors.append(f"RSS error on {feed_name}: {e}")
                
            time.sleep(1) 

    except Exception as e:
        log_callback(f"❌ Critical Stocks Scan Error: {e}")
        scan_errors.append(f"Critical stocks scan error: {e}")
    finally:
        log_callback(f"├── 🛑 Closing Chrome Driver...")
        market_utils.force_quit_driver(driver)

    return {"articles": found_reports, "errors": scan_errors}

def run_company_specific_scan(target_date, ticker_list, max_pages, log_callback, db=None, cache_map=None, existing_titles=None, lookback_start=None, lookback_end=None, trading_session_date=None):
    """
    Company Specific Hunter (Yahoo Finance).
    Fetches news for a specific list of tickers.
    """
    found_reports = []
    seen_urls = set()
    
    # Pre-load cache keys if map provided
    if cache_map:
        for u in cache_map: seen_urls.add(u)
    
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
    
    log_callback(f"\n📂 ACTIVATING PHASE: COMPANY SPECIFIC SCANNER")
    log_callback(f"├── 🎯 Targets: {', '.join(ticker_list)}")
    log_callback(f"├── 🖥️ Launching Chrome Driver...")
    
    driver = None # Initialize driver outside the try block
    try:
        driver = market_utils.get_selenium_driver()
        log_callback(f"├── 🚀 Browser Launched (PID: {driver.service.process.pid})")
    except Exception as e:
        log_callback(f"❌ Failed to launch Chrome: {str(e)}")
        return []

    # Init Progress Manager
    from modules.utils.scan_progress import ScanProgressManager
    pm = ScanProgressManager()
    
    # 🧠 Smart Tracking Init (Heuristic)
    curr_state = pm.load_state()
    if not curr_state.get("active_scan"):
            pm.start_new_scan("STOCKS", ticker_list, target_date.strftime("%Y-%m-%d"))
    
    # If resuming, we rely on the input list being the 'remaining' list
    
    try:
        for ticker in ticker_list:
            # TRACKING START
            pm.mark_target_start(ticker)
            category_tag = ticker # Fix NameError

            log_callback(f"├── 🏢 Hunting for: {ticker}...")
            
            # Construct Standard Google RSS URL for Ticker
            # Query: "{TICKER} stock news"
            # This allows Google to find the best sources (Reuters, CNBC, Yahoo, etc.)
            log_callback(f"│   ├── 🔎 RSS Search: '{ticker} stock news'...")
            query = f'{ticker} stock news'
            rss_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
            
            try:
                resp = requests.get(rss_url, headers=market_utils.HEADERS, timeout=10)
                soup = BeautifulSoup(resp.content, features="xml")
                items = soup.find_all("item")
                
                if not items:
                    log_callback(f"│   └── ⚠️ No news found for {ticker}.")
                    continue
                
                log_callback(f"│   ├── 📥 Found {len(items)} entries.")
                
                for item in items[:max_pages*10]: # Limit depth per ticker
                    title = item.title.text
                    pub_date_str = item.pubDate.text
                    google_link = item.link.text
                    
                    try:
                        pub_dt = parser.parse(pub_date_str)
                        pub_date_only = pub_dt.date()
                    except:
                        continue 
                    
                    # DATE/TIME CHECK (Sliding Window)
                    if lookback_start:
                        if pub_dt < lookback_start:
                            continue
                            
                    if lookback_end:
                        if pub_dt > lookback_end:
                            continue
                            
                    if not lookback_start and not lookback_end:
                        # Fallback to strict date check
                        if pub_date_only != target_date:
                            continue
                    
                    if title in seen_titles:
                         log_callback(f"│   └── ⏭️ Skipping known title: {title[:30]}...")
                         continue
                    
                    # Normalize Check
                    norm_title = market_utils.normalize_title(title).lower()
                    
                    if norm_title in seen_titles:
                         # Audit Trail: Show DB ID
                         db_id = seen_titles[norm_title] if isinstance(seen_titles, dict) else "?"
                         log_callback(f"│   └── ⏭️ Skipping: '{title[:40]}...' (Found in DB Row #{db_id})")
                         continue

                    # seen_titles.add(check_title) -> Moved to Success

                    # 🌍 INSTANT FOREIGN TITLES CHECK
                    upper_title = title.upper()
                    foreign_markers = ["YAHOO FINANCE UK", "YAHOO! FINANCE CANADA", "YAHOO FINANCE AUSTRALIA", "YAHOO FINANCE SINGAPORE"]
                    is_foreign = False
                    for marker in foreign_markers:
                        if marker in upper_title:
                             log_callback(f"│   └── 🛑 Skipped Foreign Source: '{title[:40]}...' (Detected: {marker})")
                             is_foreign = True
                             break
                    if is_foreign: continue

                    # 🚫 FAST BLOCKLIST (Title Scan)
                    is_blacklisted = False
                    block_reason = ""
                    for bad_src in market_utils.BLOCKED_SOURCES:
                        if bad_src in upper_title:
                            # 🛡️ EXCEPTION: Event Watch allowing ZACKS
                            if "EVENT_WATCH" in category_tag and bad_src == "ZACKS":
                                continue
                            is_blacklisted = True
                            block_reason = bad_src
                            break
                    
                    if is_blacklisted:
                        log_callback(f"│   └── 🛑 Skipped: '{title[:40]}...' (Matches Blocklist: {block_reason})")
                        continue

                    log_callback(f"│   ├── 🔹 CANDIDATE: '{title}'")
                    
                    real_url = market_utils.decode_google_news_url(google_link)
                    clean_url = real_url.split('?')[0]
                    
                    # 🚀 URL DEDUP: In-memory check replaces per-article DB query.
                    # INSERT OR IGNORE on UNIQUE url column is the DB-level safety net.
                    if clean_url in seen_urls or real_url in seen_urls:
                         log_callback(f"│   └── ⏭️ Skipping '{title[:30]}...' (URL in session cache)")
                         continue
                    
                    # 🛑 STRICT PRE-FLIGHT DOMAIN CHECK
                    try:
                        d_parts = urlparse(clean_url)
                        domain = d_parts.netloc.lower()
                        if "yahoo.com" in domain:
                            if domain not in ["finance.yahoo.com", "www.finance.yahoo.com"]:
                                log_callback(f"│   │   └── 🛑 SKIPPING: Non-US Domain detected ({domain})")
                                continue
                    except:
                        pass

                    if clean_url in seen_urls: 
                         log_callback(f"│   └── ⏭️ Skipping duplicate URL (Session Cache).")
                         continue
                    seen_urls.add(clean_url)

                    url_valid = False
                    content = None
                    
                    # SMART RETRY LOGIC
                    max_attempts = 1
                    if market_utils.is_premium_source(title, real_url):
                        max_attempts = 2
                        log_callback(f"│   │   └── 🌟 Premium Source Detected. Retries Enabled.")

                    # 🔄 RETRY LOOP (Start)
                    for attempt in range(max_attempts):
                        try:
                            # DYNAMIC ALLOW LIST

                            content = market_utils.fetch_yahoo_selenium(driver, real_url, log_callback)
                            if content:
                                url_valid = True
                                break
                        except market_utils.DeadDriverException:
                            log_callback(f"│   │   └── 💀 Browser Died (Attempt {attempt+1}/{max_attempts}). Restarting...")
                            market_utils.force_quit_driver(driver)
                            driver = market_utils.get_selenium_driver()
                        except market_utils.BlockedContentException as be:
                             log_callback(f"│   │   └── 🛑 {str(be)}")
                             break 
                        except Exception as e:
                            log_callback(f"│   │   └── ⚠️ Fetch Error (Attempt {attempt+1}/3): {e}")
                    # 🔄 RETRY LOOP (End)

                    if url_valid and content:
                        # 🛡️ REDUNDANT FINAL CHECK (Paranoid Mode)
                        try:
                            final_u = driver.current_url
                            f_p = urlparse(final_u)
                            f_d = f_p.netloc.lower()
                            if "yahoo.com" in f_d and f_d not in ["finance.yahoo.com", "www.finance.yahoo.com"]:
                                log_callback(f"│   │   └── 🛑 FINAL SECURITY BLOCK: Discarding non-US content from {f_d}")
                                continue
                        except:
                            pass

                        # UNPACK DICT (Safely)
                        if isinstance(content, list):
                            content_list = content
                            publisher = "Yahoo Finance" # Default if not in dict
                        else:
                            content_list = content.get("content", [])
                            publisher = content.get("publisher", "Yahoo Finance")

                        # --- STRICT RELEVANCE CHECK ---
                        # Verify the Ticker is actually mentioned in Title or Content
                        relevant = False
                        if ticker.upper() in title.upper():
                            relevant = True
                        if not relevant:
                            # Use content_list now (first 5 lines)
                            snippet = " ".join(content_list[:5]).upper()
                            if ticker.upper() in snippet:
                                relevant = True
                                
                        if relevant:
                            report_item = {
                                "title": title,
                                "url": real_url,
                                "content": content_list,
                                "publisher": publisher,
                                "time": pub_dt.strftime("%H:%M %Z%z").strip(),
                                "published_at": pub_dt.isoformat(),
                                "source_domain": "finance.yahoo.com",
                                "category": ticker 
                            }
                            
                            # 💾 INCREMENTAL SAVE
                            if db:
                                inserted_count, dups_count = db.insert_news([report_item], ticker, trading_session_date=trading_session_date)
                                if inserted_count > 0:
                                    log_callback(f"│   │   └── 💾 SAVED to DB immediately.")
                                elif dups_count > 0:
                                    log_callback(f"│   │   └── ⚠️ Already Exists (Ignored by DB).")
                                else:
                                    log_callback(f"│   │   └── ⚠️ DB Insert Failed.")

                            found_reports.append(report_item)
                            log_callback(f"│   │   └── 🏆 REPORT SECURED! [Source: {publisher}]")
                        else:
                            log_callback(f"│   │   └── 🗑️ DISCARDED: Ticker '{ticker}' not found in Title/Intro.") # This line seems out of place for run_stocks_scan feed loop, but was in file. Keeping for safety.
                    else:
                        log_callback(f"│   │   └── ⚠️ No content extracted.")

            except Exception as e:
                log_callback(f"│   └── ❌ Stock RSS Error: {str(e)}")
            
            # TRACKING COMPLETE
            pm.mark_target_complete(ticker)

             # 🧹 SANITIZE DRIVER
            try: 
                if driver: driver.get("about:blank")
            except: pass
            
            time.sleep(1)

    except Exception as e:
        log_callback(f"❌ Critical Stock Scan Error: {e}")
    finally:
        if driver: market_utils.force_quit_driver(driver)
        
        # 🏁 ONLY FINISH if we actually went through the targets
        if 'active_targets' in locals() and len(active_targets) > 0:
             pm.finish_scan()

        # Also finish for company scan
        if 'ticker_list' in locals() and len(ticker_list) > 0:
             pm.finish_scan()
            
    return found_reports