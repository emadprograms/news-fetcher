
# investpy removed due to extreme Cloudflare blocking
from bs4 import BeautifulSoup
import time

import datetime
from . import db_client

# Static List of Mega-Cap Companies (~$200B+ and Market Movers)
MEGA_CAP_TICKERS = {
    # MAG 7 & TECH
    "AAPL", "MSFT", "NVDA", "GOOG", "GOOGL", "AMZN", "META", "TSLA",
    "AVGO", "ORCL", "CRM", "ADBE", "AMD", "QCOM", "TXN", "INTC", "IBM", "UBER",
    "CSCO", "INTU", "NOW", "AMAT", "MU", "PANW", "SNOW", "PLTR",
    
    # FINANCIALS
    "JPM", "BAC", "WFC", "C", "MS", "GS", "BLK", "V", "MA", "AXP", "PYPL", "HOOD",
    "BRK.A", "BRK.B",
    
    # OIL & ENERGY
    "XOM", "CVX", "SHEL", "TTE", "COP", "BP", "OXY", "SLB",
    
    # PHARMA & HEALTH
    "LLY", "UNH", "JNJ", "ABBV", "MRK", "NVO", "PFE", "AMGN", "TMO", "DHR",
    
    # RETAIL & CONSUMER
    "WMT", "COST", "PG", "HD", "KO", "PEP", "MCD", "NKE", "SBUX", "DIS",
    
    # INDUSTRIAL
    "CAT", "DE", "GE", "UNP", "UPS", "BA", "LMT", "RTX",
    
    # CHIPS & SEMIS
    "TSM", "ASML", "ARM", 
    
    # OTHER MAJORS
    "NFLX", "CMCSA", "TMUS", "VZ", "T"
}

# Yahoo Calendar URLs
ECO_CALENDAR_URL = "https://finance.yahoo.com/calendar/economic"
EARNINGS_CALENDAR_URL = "https://finance.yahoo.com/calendar/earnings"

from modules.utils.market_utils import get_selenium_driver, force_quit_driver, HEADERS # Reuse existing driver factory and headers

class CalendarPopulator:
    def __init__(self, db: db_client.NewsDatabase, analyst_db=None):
        self.db = db
        self.analyst_db = analyst_db

    def sync_week(self, base_date=None):
        """ Clears and repopulates the calendar for the specific week (Mon-Sun) of the base_date. """
        if base_date is None:
            base_date = datetime.date.today()
            
        # 🗓️ Logic: Snap to Start of Week (Monday)
        # weekday(): Mon=0 ... Sun=6
        start_of_week = base_date - datetime.timedelta(days=base_date.weekday())
        
        print(f"📅 Syncing Window: Mon {start_of_week} -> Sun {start_of_week + datetime.timedelta(days=6)}")
        
        # 1. Clear Old Data
        self.db.clear_calendar()
        
        # 2. Fetch Economic Events (Pass Monday)
        eco_events = self.fetch_economic_calendar(start_of_week)
        c1 = self.db.insert_calendar_events(eco_events)
        print(f"✅ Inserted {c1} Economic Events.")

        # 3. Fetch Earnings (Pass Monday)
        earn_events = self.fetch_earnings_calendar(start_of_week)
        c2 = self.db.insert_calendar_events(earn_events)
        print(f"✅ Inserted {c2} Earnings Events.")
        
        return c1 + c2

    def fetch_economic_calendar(self, start_date):
        """ Scrapes Economic Calendar from Yahoo Finance using Selenium (Alternative to InvestPy). """
        events = []
        driver = None
        try:
            print("🚀 Launching Browser for Economic Data Sync...")
            driver = get_selenium_driver(headless=True)
            
            # Fetch Monday -> Friday (5 Days)
            for i in range(5):
                target_date = start_date + datetime.timedelta(days=i)
                date_str = target_date.strftime("%Y-%m-%d")
                url = f"{ECO_CALENDAR_URL}?day={date_str}"
                
                try:
                    print(f"   -> Scraping Economic Events for {date_str}...")
                    driver.set_page_load_timeout(30)
                    driver.get(url)
                    time.sleep(3) # Allow JS to load table
                    
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    
                    # 🔍 Dynamic Header Mapping
                    headers = [th.get_text().strip().lower() for th in soup.find_all("th")]
                    idx_time = 0
                    idx_event = 1
                    idx_country = 2
                    
                    for idx, h in enumerate(headers):
                        if "time" in h: idx_time = idx
                        elif "event" in h: idx_event = idx
                        elif "country" in h: idx_country = idx

                    rows = soup.find_all("tr")
                    seen_signatures = set()
                    
                    day_count = 0
                    for row in rows:
                        cols = row.find_all("td")
                        if len(cols) < 3: continue
                        
                        raw_country = cols[idx_country].get_text().strip()
                        if "US" not in raw_country and "United States" not in raw_country:
                            continue
                            
                        raw_time = cols[idx_time].get_text().strip()
                        event_name = cols[idx_event].get_text().strip()
                        
                        importance = "MEDIUM" # Default
                        high_impact_keywords = ["cpi", "ppi", "fomc", "fed", "nonfarm", "employment", "gdp", "pce", "interest rate", "jobless"]
                        if any(kw in event_name.lower() for kw in high_impact_keywords):
                            importance = "HIGH"
                            
                        # Deduplicate
                        sig = (event_name, date_str)
                        if sig in seen_signatures: continue
                        seen_signatures.add(sig)

                        events.append({
                            "name": event_name,
                            "ticker": None,
                            "type": "MACRO_EVENT",
                            "date": date_str,
                            "importance": importance,
                            "country": "US",
                            "time": raw_time if raw_time else "TBA"
                        })
                        day_count += 1
                    print(f"      Found {day_count} US economic events.")
                except Exception as day_e:
                    print(f"⚠️ Failed to scrape economic data for {date_str}: {day_e}")
                    continue
                    
        except Exception as e:
            print(f"⚠️ Economic Data Fetch Error: {e}")
            
        finally:
            if driver:
                print("🛑 Closing Economic Browser.")
                force_quit_driver(driver)
            
        return events

    def fetch_earnings_calendar(self, start_date):
        """ Scrapes Yahoo Earnings Calendar using Selenium (Required for Date Navigation). """
        events = []
        driver = None
        try:
            # 1. Build Whitelist
            valid_tickers = set(MEGA_CAP_TICKERS)
            if self.analyst_db:
                watched = self.analyst_db.fetch_monitored_tickers()
                valid_tickers.update(watched)
            
            print(f"🔍 Earnings Filter: Off (Capturing All). Tracking {len(valid_tickers)} Major Tickers for context.")
            print("🚀 Launching Browser for Earnings Sync (This takes ~20s)...")
            
            driver = get_selenium_driver(headless=True)
            
            # Fetch Monday -> Friday (5 Days)
            for i in range(5):
                target_date = start_date + datetime.timedelta(days=i)
                date_str = target_date.strftime("%Y-%m-%d")
                url = f"{EARNINGS_CALENDAR_URL}?day={date_str}"
                
                try:
                    print(f"   -> Scraping Earnings for {date_str}...")
                    driver.set_page_load_timeout(30) # Prevent infinite hangs
                    driver.get(url)
                    time.sleep(3) # Allow JS to load table
                    
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    
                    # 🔍 Dynamic Header Mapping
                    headers = [th.get_text().strip() for th in soup.find_all("th")]
                    idx_est = 4 # Default
                    idx_act = 5
                    idx_surp = 6
                    
                    try:
                        # Try to find exact columns
                        for i, h in enumerate(headers):
                            if "EPS Estimate" in h: idx_est = i
                            elif "Reported EPS" in h: idx_act = i
                            elif "Surprise" in h: idx_surp = i
                        print(f"      Mapped Columns: Est={idx_est}, Act={idx_act}, Surp={idx_surp}")
                    except Exception:
                        pass

                    rows = soup.find_all("tr")
                    
                    day_count = 0
                    for row in rows:
                        cols = row.find_all("td")
                        if len(cols) < 4: continue
                        
                        ticker = cols[0].get_text().strip()
                        name = cols[1].get_text().strip()
                        
                        # Time Logic
                        raw_time = cols[3].get_text().strip() if len(cols) > 3 else "TBA"
                        if raw_time == "AMC": raw_time = "After Market"
                        elif raw_time == "BMO": raw_time = "Pre Market"
                        # TAS remains TAS as requested

                        # 📊 Capture EPS Data (Dynamic Indices)
                        # Helper to safely get text
                        def get_val(idx):
                            if len(cols) > idx:
                                val = cols[idx].get_text().strip()
                                return val if val else "-"
                            return "-"

                        eps_est = get_val(idx_est)
                        eps_act = get_val(idx_act)
                        eps_surp = get_val(idx_surp)

                        events.append({
                            "name": name, # Store actual Company Name (e.g. 'GitLab Inc.')
                            "ticker": ticker,
                            "type": "EARNINGS",
                            "date": date_str,
                            "importance": "HIGH",
                            "country": "US",
                            "time": raw_time,
                            "eps_estimate": eps_est,
                            "eps_reported": eps_act,
                            "eps_surprise": eps_surp
                        })
                        day_count += 1
                    print(f"      Found {day_count} tickers.")
                except Exception as day_e:
                    print(f"⚠️ Failed to scrape {date_str}: {day_e}")
                    continue # Try next day
                
        except Exception as e:
            print(f"⚠️ Earnings Fetch Error: {e}")
            
        finally:
            if driver:
                print("🛑 Closing Earnings Browser.")
                force_quit_driver(driver)
            
        return events
