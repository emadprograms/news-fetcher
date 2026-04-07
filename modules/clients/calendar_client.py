
import investpy # 🚀 Using InvestPy as requested
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
        """ Scrapes Economic Calendar using InvestPy (Official Library). """
        events = []
        try:
            # 1. Date Range
            end_date = start_date + datetime.timedelta(days=7)
            
            from_str = start_date.strftime("%d/%m/%Y")
            to_str = end_date.strftime("%d/%m/%Y")
            
            print(f"🗓️ Syncing via InvestPy ({from_str} - {to_str})...")
            
            # 2. Fetch Data
            df = investpy.economic_calendar(
                countries=['united states'],
                from_date=from_str,
                to_date=to_str,
                time_zone='GMT'
            )
            
            # 3. Process Rows
            seen_signatures = set()
            
            for index, row in df.iterrows():
                # 🛡️ DOUBLE CHECK: US Only
                # Ensure case-insensitive check
                zone = row['zone'].lower() if row['zone'] else ""
                if zone != 'united states': continue

                event_name = row['event']
                date_raw = row['date'] # "dd/mm/yyyy"
                
                # Convert date to YYYY-MM-DD for consistency
                dt_obj = datetime.datetime.strptime(date_raw, "%d/%m/%Y").date()
                date_iso = dt_obj.strftime("%Y-%m-%d")

                # Handle missing importance efficiently (pandas sometimes returns NaN which is a float)
                raw_importance = row['importance']
                if not isinstance(raw_importance, str) or not raw_importance:
                    importance = "LOW"
                else:
                    importance = str(raw_importance).upper()
                
                # Deduplicate
                sig = (event_name, date_iso)
                if sig in seen_signatures: continue
                seen_signatures.add(sig)

                # Filter Logic
                if importance not in ["HIGH", "MEDIUM"]: continue 

                events.append({
                    "name": event_name,
                    "ticker": None,
                    "type": "MACRO_EVENT",
                    "date": date_iso,
                    "importance": importance,
                    "country": "US", # Explicitly tag as US
                    "time": str(row['time']) if row['time'] else "TBA"
                })
                
        except Exception as e:
            print(f"⚠️ InvestPy Fetch Error: {e}")
            
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
