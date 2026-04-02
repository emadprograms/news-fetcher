import datetime
import logging
import sys
import os
import time
import traceback
import requests

# Ensure the root directory is in the path so we can import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modules.engines import macro_engine
from modules.engines import stocks_engine
from modules.engines import marketaux_engine
from modules.clients.db_client import NewsDatabase
from modules.clients.infisical_client import InfisicalManager
from modules.clients.calendar_client import CalendarPopulator
import modules.utils.market_utils as market_utils

# Configure logging
LOGS_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
APP_LOGS_DIR = os.path.join(LOGS_ROOT, "app")
SYS_LOGS_DIR = os.path.join(LOGS_ROOT, "system")

for d in [APP_LOGS_DIR, SYS_LOGS_DIR]:
    if not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
log_filename = os.path.join(APP_LOGS_DIR, f"automation_{timestamp}_UTC.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_filename, mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)

def update_log(message):
    logging.info(message)

def cleanup_logs(days_to_keep=7):
    """ Deletes log files older than X days to keep things tidy. """
    try:
        now = time.time()
        seconds_back = days_to_keep * 86400
        count = 0
        
        # Clean App Logs and System Logs
        for folder in [APP_LOGS_DIR, SYS_LOGS_DIR]:
            if not os.path.exists(folder): continue
            for f in os.listdir(folder):
                if f.endswith(".log"):
                    path = os.path.join(folder, f)
                    if os.stat(path).st_mtime < now - seconds_back:
                        os.remove(path)
                        count += 1
        if count > 0:
            update_log(f"🧹 Cleaned up {count} old log files from app/system folders.")
    except Exception as e:
        update_log(f"⚠️ Log cleanup failed: {e}")

def send_discord_report(webhook_url, message, embeds=None):
    """ Sends a notification to Discord. Supports rich embeds. """
    if not webhook_url: return
    try:
        if embeds:
            payload = {"embeds": embeds}
        else:
            payload = {"content": message}
        requests.post(webhook_url, json=payload, timeout=10)
    except Exception as e:
        update_log(f"⚠️ Discord notification failed: {e}")

def build_discord_report(target_date, lookback_start, lookback_end, report, duration_sec, run_number=1, max_runs=3):
    """
    Builds a rich Discord embed with categorized alerting.
    Returns (message_text, embed_list).
    """
    macro = report.get("macro", 0)
    stocks = report.get("stocks", 0)
    company = report.get("company", 0)
    total = macro + stocks + company
    total_db = report.get("total_in_db", 0)
    cal_events = report.get("calendar_events", 0)
    ma_keys = report.get("marketaux_keys", 0)
    tickers_count = report.get("tickers_scanned", 0)
    errors = report.get("errors", [])
    
    minutes = int(duration_sec // 60)
    seconds = int(duration_sec % 60)
    
    # Categorize errors into warnings and criticals
    critical_keywords = ["crashed", "failed", "driver died", "reboot failed", "connection refused", "aborted", "critical"]
    warning_keywords = ["missing", "not found", "skipping", "sync failed", "timeout", "headline only", "no content"]
    
    criticals = []
    warnings = []
    for e in errors:
        e_lower = e.lower()
        if any(w in e_lower for w in critical_keywords):
            criticals.append(e)
        elif any(w in e_lower for w in warning_keywords):
            warnings.append(e)
        else:
            criticals.append(e)  # Default unknown errors to critical
    
    # Determine embed color (Discord color codes)
    if criticals:
        color = 0xFF0000  # Red
        status_line = f"\U0001f6a8 CRITICAL ISSUES DETECTED ({len(criticals)} errors)"
    elif warnings:
        color = 0xFFAA00  # Orange/Yellow
        status_line = f"\u26a0\ufe0f Completed with {len(warnings)} warning(s)"
    else:
        color = 0x00FF00  # Green
        status_line = "\u2705 All systems nominal"
    
    # Build Description
    start_str = lookback_start.strftime("%a %b %d, %H:%M UTC")
    end_str = lookback_end.strftime("%a %b %d, %H:%M UTC")
    
    desc_lines = [
        f"🗓️ **Session:** {start_str} \u2192 {end_str}",
        f"\U0001f30d **Macro:** {macro}  |  \U0001f4c8 **Stocks:** {stocks}  |  \U0001f3e2 **Company:** {company}",
        f"\U0001f4f0 **New in Hunt:** {total} articles",
    ]
    if total_db > 0:
        desc_lines.append(f"\U0001f5c4\ufe0f **Session Total:** {total_db} articles")
    if cal_events > 0:
        desc_lines.append(f"\U0001f4c5 **Calendar:** {cal_events} events synced")
    if tickers_count > 0:
        desc_lines.append(f"\U0001f3af **Tickers:** {tickers_count}  |  \U0001f511 **Keys:** {ma_keys}")
    desc_lines.append(f"\u23f1\ufe0f **Duration:** {minutes}m {seconds}s  |  **Run:** {run_number}/{max_runs}")
    
    # Show error count in description if any errors exist
    if errors:
        desc_lines.append(f"\n\u26a0\ufe0f **{len(errors)} issue(s) detected** — see below")
    
    embed = {
        "title": f"\U0001f9b5 GRANDMASTER HUNT \u2014 {target_date}",
        "description": "\n".join(desc_lines),
        "color": color,
        "footer": {"text": status_line}
    }
    
    # Add error fields
    if criticals:
        embed["fields"] = embed.get("fields", [])
        embed["fields"].append({
            "name": f"\U0001f6a8 Critical ({len(criticals)})",
            "value": "\n".join([f"\u274c {e}" for e in criticals[:5]]),
            "inline": False
        })
    if warnings:
        embed["fields"] = embed.get("fields", [])
        embed["fields"].append({
            "name": f"\u26a0\ufe0f Warnings ({len(warnings)})",
            "value": "\n".join([f"\u2022 {w}" for w in warnings[:5]]),
            "inline": False
        })
    
    return None, [embed]

def run_automation(run_number=1, max_runs=3):
    """
    Main automation orchestrator.
    Returns a result dict: {"success": bool, "articles_found": int, "errors": list}
    """
    start_time = time.time()
    update_log("🚀 INITIATING AUTOMATED GRANDMASTER HUNT PROTOCOL (MARKET-CENTRIC DAY)")
    
    # 🕒 REF-LOGIC: POST-MARKET-TO-POST-MARKET SESSIONS
    # ------------------------------------------------------------------
    # Session for trading day T:
    #   Start: (prev_trading_day(T) + 1 day) @ 1 AM UTC
    #   End:   (T + 1 day) @ 1 AM UTC
    # Weekends/Holidays absorbed into the next trading day's session.
    # ------------------------------------------------------------------
    
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    
    # Handle optional manual date override (from GitHub Actions)
    manual_date_str = os.environ.get("TARGET_DATE", "").strip()
    
    if manual_date_str:
        try:
            manual_date = datetime.datetime.strptime(manual_date_str, "%Y-%m-%d").date()
            update_log(f"⚠️ MANUAL DATE OVERRIDE DETECTED: {manual_date}")
            target_date, lookback_start, lookback_end = market_utils.MarketCalendar.resolve_session_for_date(manual_date, now_utc)
            if target_date != manual_date:
                update_log(f"📅 Resolved non-trading date {manual_date} → session for trading day {target_date}")
        except ValueError:
            update_log(f"❌ Invalid TARGET_DATE format '{manual_date_str}'. Falling back to automatic logic.")
            manual_date_str = None
            
    if not manual_date_str:
        # Automatic session resolution: post-market close to post-market close
        target_date, lookback_start, lookback_end = market_utils.MarketCalendar.resolve_trading_session(now_utc)

    switch_hour = market_utils.MarketCalendar.get_premarket_switch_hour_utc(target_date)
    update_log(f"⏰ TRADING DATE FOCUS: {target_date} (Switch Hour: {switch_hour}:00 UTC)")
    update_log(f"⏰ LOOKBACK WINDOW (UTC): {lookback_start.strftime('%Y-%m-%d %H:%M')} -> {lookback_end.strftime('%Y-%m-%d %H:%M')}")
    
    # Early Close info
    if market_utils.MarketCalendar.is_early_close(target_date):
        update_log(f"⚠️ NOTE: {target_date} is an NYSE Early Close day (1 PM EST).")
    
    # Report tracker
    report = {
        "macro": 0,
        "stocks": 0,
        "company": 0,
        "calendar_events": 0,
        "marketaux_keys": 0,
        "tickers_scanned": 0,
        "total_in_db": 0,
        "errors": []
    }
    
    infisical = InfisicalManager()
    if not infisical.is_connected:
        update_log("❌ Error: Infisical not connected. Check credentials.")
        report["errors"].append("Infisical connection failed")
        return {"success": False, "articles_found": 0, "errors": report["errors"]}

    # Fetch Discord Webhook
    discord_webhook = infisical.get_discord_webhook()
    
    # 0. Cleanup Old Logs
    cleanup_logs(7)

    # 1. Initialize Databases
    db = None
    analyst_db = None
    try:
        db_url, db_token = infisical.get_turso_news_credentials()
        if db_url and db_token:
            db = NewsDatabase(db_url, db_token)
            update_log("✅ News DB Online")
        else:
            update_log("⚠️ News DB Credentials Missing")
            report["errors"].append("News DB credentials missing from Infisical")
            
        a_url, a_token = infisical.get_turso_analyst_credentials()
        if a_url and a_token:
            analyst_db = NewsDatabase(a_url, a_token, init_schema=False)
            update_log("✅ Analyst DB Online")
    except Exception as e:
        update_log(f"❌ Database Initialization Failed: {e}")
        report["errors"].append(f"Database init failed: {e}")
        send_discord_report(discord_webhook, *build_discord_report(target_date, lookback_start, lookback_end, report, time.time() - start_time))
        return {"success": False, "articles_found": 0, "errors": report["errors"]}

    if not db:
        update_log("❌ Error: News Database is required for scan result persistence.")
        report["errors"].append("News DB is required but unavailable")
        send_discord_report(discord_webhook, *build_discord_report(target_date, lookback_start, lookback_end, report, time.time() - start_time))
        return {"success": False, "articles_found": 0, "errors": report["errors"]}

    # 1.5 Sync Calendar
    try:
        update_log("📅 Syncing Economic & Earnings Calendar...")
        cal_pop = CalendarPopulator(db, analyst_db=analyst_db)
        cal_count = cal_pop.sync_week()
        report["calendar_events"] = cal_count if cal_count else 0
        update_log("✅ Calendar Sync Complete.")
    except Exception as e:
        update_log(f"⚠️ Calendar Sync Failed: {e}")
        report["errors"].append(f"Calendar sync failed: {e}")

    # (Removed unused articles_before snapshot — each scan tracks its own before/after)

    # 🚀 OPTIMIZATION: Load dedup context ONCE and reuse across all scan phases.
    # After each scan, merge newly found titles into existing_titles so subsequent
    # phases have fresh dedup data without re-querying the database.
    iso_start = lookback_start.isoformat()
    iso_end = lookback_end.isoformat()
    existing_titles = db.fetch_existing_titles_range(iso_start, iso_end)
    cache = db.fetch_cache_map(target_date, None)
    update_log(f"📦 Dedup Context Loaded: {len(existing_titles)} titles, {len(cache)} cached URLs.")

    # 2. Run Macro Scan
    try:
        update_log("\U0001f30d Starting Macro Scan...")
        macro_result = macro_engine.run_macro_scan(
            target_date, 
            max_pages=5, 
            log_callback=update_log, 
            db=db, 
            cache_map=cache, 
            existing_titles=existing_titles,
            headless=True,
            lookback_start=lookback_start,
            lookback_end=lookback_end,
            trading_session_date=target_date
        )
        # Handle structured return: {"articles": [...], "errors": [...]}
        if isinstance(macro_result, dict):
            macro_articles = macro_result.get("articles", [])
            macro_errors = macro_result.get("errors", [])
            report["errors"].extend(macro_errors)
        else:
            macro_articles = macro_result if macro_result else []  # Legacy fallback
        report["macro"] = len(macro_articles)
        # Merge newly found titles into shared dedup context for next phase
        for art in macro_articles:
            norm_t = market_utils.normalize_title(art.get('title', '')).lower()
            existing_titles[norm_t] = "macro_phase"
            url = art.get('url')
            if url:
                cache[url] = True
        # Check for headline-only ratio (driver issues indicator)
        if macro_articles:
            timeout_count = sum(1 for a in macro_articles if a.get("publisher", "") in ["Unknown (Timeout)", "Unknown (Error)"])
            if timeout_count > 0:
                ratio = timeout_count / len(macro_articles) * 100
                if ratio > 50:
                    report["errors"].append(f"Macro: {int(ratio)}% of articles had no content extracted (likely driver issues)")
        update_log(f"\u2705 Macro Scan Complete. {report['macro']} new articles.")
    except Exception as e:
        update_log(f"\u274c Macro Scan Failed: {e}")
        report["errors"].append(f"Macro scan crashed: {e}")

    # 3. Run Stocks Scan (reuses existing_titles + cache from macro phase — no DB reload)
    try:
        update_log("\U0001f4c8 Starting Stocks Scan...")
        stocks_result = stocks_engine.run_stocks_scan(
            target_date, 
            max_pages=5, 
            log_callback=update_log, 
            db=db, 
            cache_map=cache, 
            existing_titles=existing_titles,
            headless=True,
            lookback_start=lookback_start,
            lookback_end=lookback_end,
            trading_session_date=target_date
        )
        # Handle structured return: {"articles": [...], "errors": [...]}
        if isinstance(stocks_result, dict):
            stocks_articles = stocks_result.get("articles", [])
            stocks_errors = stocks_result.get("errors", [])
            report["errors"].extend(stocks_errors)
        else:
            stocks_articles = stocks_result if stocks_result else []  # Legacy fallback
        report["stocks"] = len(stocks_articles)
        # Merge newly found titles into shared dedup context for company phase
        for art in stocks_articles:
            norm_t = market_utils.normalize_title(art.get('title', '')).lower()
            existing_titles[norm_t] = "stocks_phase"
            url = art.get('url')
            if url:
                cache[url] = True
        # Check for headline-only ratio
        if stocks_articles:
            timeout_count = sum(1 for a in stocks_articles if a.get("publisher", "") in ["Unknown (Timeout)", "Unknown (Error)"])
            if timeout_count > 0:
                ratio = timeout_count / len(stocks_articles) * 100
                if ratio > 50:
                    report["errors"].append(f"Stocks: {int(ratio)}% of articles had no content extracted (likely driver issues)")
        update_log(f"\u2705 Stocks Scan Complete. {report['stocks']} new articles.")
    except Exception as e:
        update_log(f"\u274c Stocks Scan Failed: {e}")
        report["errors"].append(f"Stocks scan crashed: {e}")

    # 4. Run Company Specific Scan (MarketAux) — reuses shared dedup context
    try:
        update_log("🏢 Starting Company Specific Scan...")
        ma_keys = infisical.get_marketaux_keys()
        report["marketaux_keys"] = len(ma_keys)
        if not ma_keys:
            update_log("⚠️ MarketAux API Keys missing. Skipping company scan.")
            report["errors"].append("MarketAux API keys not found in Infisical")
        else:
            # Fetch monitored tickers from analyst DB
            tickers = []
            if analyst_db:
                tickers = analyst_db.fetch_monitored_tickers()
            
            report["tickers_scanned"] = len(tickers)
            
            if not tickers:
                update_log("ℹ️ No monitored tickers found in Analyst DB.")
                report["errors"].append("No monitored tickers in Analyst DB for MarketAux scan")
            else:
                ma_results = marketaux_engine.run_marketaux_scan(
                    ma_keys, 
                    target_date, 
                    tickers, 
                    update_log, 
                    db=db, 
                    cache_map=cache, 
                    existing_titles=existing_titles,
                    headless=True,
                    lookback_start=lookback_start,
                    lookback_end=lookback_end,
                    trading_session_date=target_date
                )
                report["company"] = len(ma_results) if ma_results else 0
                update_log(f"✅ Company Scan Complete. {report['company']} new articles.")
    except Exception as e:
        update_log(f"❌ Company Scan Failed: {e}")
        report["errors"].append(f"Company scan crashed: {e}")

    # Final count: total articles in session window
    # 📝 HUNT HEARTBEAT: Log Start
    hunt_id = db.log_hunt_start(run_number, target_date, lookback_start, lookback_end)
    update_log(f"💓 Hunt Heartbeat Started (ID: {hunt_id}, Run: {run_number}/{max_runs})")

    iso_start = lookback_start.isoformat()
    iso_end = lookback_end.isoformat()
    report["total_in_db"] = db.count_news_range(iso_start, iso_end)
    update_log(f"📦 Total articles in session window: {report['total_in_db']}")

    # Final Report
    total_found = report["macro"] + report["stocks"] + report["company"]
    duration = time.time() - start_time
    update_log("🏁 GRANDMASTER HUNT COMPLETE.")
    
    msg_text, embeds = build_discord_report(target_date, lookback_start, lookback_end, report, duration, run_number, max_runs)
    send_discord_report(discord_webhook, msg_text, embeds)

    # 📝 HUNT HEARTBEAT: Log End
    hunt_status = "SUCCESS" if not report["errors"] else ("PARTIAL" if total_found > 0 else "FAILED")
    db.log_hunt_end(hunt_id, hunt_status, total_found, report["total_in_db"], duration, report["errors"] or None)
    update_log(f"💓 Hunt Heartbeat Finalized: {hunt_status}")

    # Properly Close Databases
    try:
        db.close()
        if analyst_db:
            analyst_db.close()
    except:
        pass

    # Return result for multi-run logic
    return {
        "success": not report["errors"],
        "articles_found": total_found,
        "errors": report["errors"]
    }

def run_check_only():
    """
    Minimal run that only checks the session status and database count.
    Used by the Discord bot via GitHub Actions to avoid direct DB load from the bot.
    """
    update_log("🔍 INITIATING SESSION STATUS CHECK...")
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    
    # Resolve Session
    manual_date_str = os.environ.get("TARGET_DATE", "").strip()
    if manual_date_str:
        try:
            manual_date = datetime.datetime.strptime(manual_date_str, "%Y-%m-%d").date()
            target_date, lookback_start, lookback_end = market_utils.MarketCalendar.resolve_session_for_date(manual_date, now_utc)
        except ValueError:
            target_date, lookback_start, lookback_end = market_utils.MarketCalendar.resolve_trading_session(now_utc)
    else:
        target_date, lookback_start, lookback_end = market_utils.MarketCalendar.resolve_trading_session(now_utc)

    # Connect to DB & Infisical
    infisical = InfisicalManager()
    if not infisical.is_connected:
        update_log("❌ Infisical Connection Failed")
        return
        
    webhook = infisical.get_discord_webhook()
    db_url, db_token = infisical.get_turso_news_credentials()
    
    if not db_url or not db_token:
        update_log("❌ DB Credentials Missing")
        return
        
    db = NewsDatabase(db_url, db_token, init_schema=False)
    iso_start = lookback_start.isoformat()
    iso_end = lookback_end.isoformat()
    total_db = db.count_news_range(iso_start, iso_end)
    
    # Build Embed
    start_str = lookback_start.strftime("%a %b %d, %H:%M UTC")
    end_str = lookback_end.strftime("%a %b %d, %H:%M UTC")
    
    embed = {
        "title": f"📊 Session Status: {target_date}",
        "description": (
            f"🗓️ **Start:** `{start_str}`\n"
            f"🗓️ **End:** `{end_str}`\n\n"
            f"📰 **Articles in DB:** `{total_db}`"
        ),
        "color": 0x3498db, # Blue
        "footer": {"text": "NewsFetcher Live Grid | Status Query"}
    }
    
    send_discord_report(webhook, None, [embed])
    update_log(f"✅ Status Check Complete: {total_db} articles found.")
    
    # Properly Close DB
    try:
        db.close()
    except:
        pass

MAX_HUNT_RUNS = 3
COOLDOWN_BETWEEN_RUNS = 30  # seconds

if __name__ == "__main__":
    # Check if we are in "CHECK" mode
    if os.environ.get("MODE") == "CHECK":
        run_check_only()
        update_log("🎬 STATUS CHECK FINISHED.")
        os._exit(0) # Force exit to prevent hanging on unclosed aiohttp sessions

    for run_num in range(1, MAX_HUNT_RUNS + 1):
        try:
            update_log(f"\n{'='*50}")
            update_log(f"🔁 HUNT ATTEMPT {run_num}/{MAX_HUNT_RUNS}")
            update_log(f"{'='*50}")
            
            result = run_automation(run_number=run_num, max_runs=MAX_HUNT_RUNS)
            
            if result and result.get("success"):
                update_log(f"✅ Run {run_num} completed successfully. No need to retry.")
                break
            else:
                errors = result.get("errors", []) if result else ["Unknown failure"]
                update_log(f"⚠️ Run {run_num} completed with {len(errors)} error(s).")
                if run_num < MAX_HUNT_RUNS:
                    update_log(f"⏳ Cooling down {COOLDOWN_BETWEEN_RUNS}s before next attempt...")
                    time.sleep(COOLDOWN_BETWEEN_RUNS)
                    
        except Exception as e:
            error_details = traceback.format_exc()
            update_log(f"🚨 Run {run_num} CRASHED: {e}")
            
            if run_num == MAX_HUNT_RUNS:
                # Final attempt failed — send emergency Discord alert
                try:
                    infisical = InfisicalManager()
                    webhook = infisical.get_discord_webhook()
                    if webhook:
                        target_date = datetime.datetime.now(datetime.timezone.utc).date()
                        emergency_embed = {
                            "title": f"🚨 CRITICAL SYSTEM FAILURE — {target_date}",
                            "description": (
                                f"The Grandmaster Hunt has crashed on **all {MAX_HUNT_RUNS} attempts**!\n\n"
                                f"**Error:** `{str(e)}`\n"
                                f"**Details:**\n```python\n{error_details[:500]}...\n```\n"
                                f"🔍 Check `logs/app/` for the full trace."
                            ),
                            "color": 0xFF0000  # Red
                        }
                        requests.post(webhook, json={"embeds": [emergency_embed]}, timeout=10)
                except Exception:
                    pass
                
                logging.error(f"FATAL CRASH (All {MAX_HUNT_RUNS} attempts): {e}")
                logging.error(error_details)
                sys.exit(1)
            else:
                update_log(f"⏳ Cooling down {COOLDOWN_BETWEEN_RUNS}s before retry...")
                time.sleep(COOLDOWN_BETWEEN_RUNS)
    
    update_log("🎬 ALL HUNT ATTEMPTS FINISHED.")
    os._exit(0)  # 🔒 SAFETY NET: Force-kill process to prevent zombie threads/Chrome from blocking GitHub Actions
