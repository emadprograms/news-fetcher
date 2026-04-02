import streamlit as st
import pandas as pd
import datetime
import os
import sys
import warnings
import time
from dateutil import parser as dt_parser

# --- SYSTEM SETUP ---
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

# Suppress pkg_resources deprecation warning from investpy
warnings.filterwarnings("ignore", category=UserWarning, module="investpy")
warnings.filterwarnings("ignore", message=".*pkg_resources is deprecated.*")

# Ensure the root directory is in the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modules.engines import macro_engine 
from modules.engines import stocks_engine
from modules.engines import marketaux_engine
from modules.clients.db_client import NewsDatabase
from modules.clients.infisical_client import InfisicalManager
from modules.clients.calendar_client import CalendarPopulator, MEGA_CAP_TICKERS
import modules.utils.market_utils as market_utils
from modules.utils.scan_progress import ScanProgressManager

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Grandmaster Hunt Full Dashboard",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- PREMIUM STYLING ---
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stApp { color: #e0e0e0; }
    
    /* Metric Cards */
    .metric-card {
        background-color: #1e2130;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #30363d;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    
    /* News Cards */
    .news-card {
        background-color: #161b22;
        padding: 18px;
        border-radius: 8px;
        border-left: 5px solid #238636;
        margin-bottom: 12px;
        transition: all 0.2s ease-in-out;
        border: 1px solid #30363d;
        border-left-width: 6px;
    }
    .news-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.4);
        border-color: #58a6ff;
    }
    .news-title {
        font-size: 1.15rem;
        font-weight: bold;
        color: #58a6ff;
        text-decoration: none;
        display: block;
        margin-bottom: 5px;
    }
    .news-meta {
        font-size: 0.85rem;
        color: #8b949e;
        margin-bottom: 10px;
    }
    .news-content {
        font-size: 0.95rem;
        color: #c9d1d9;
        line-height: 1.5;
    }
    
    /* Status Labels */
    .status-success { color: #238636; font-weight: bold; }
    .status-partial { color: #d29922; font-weight: bold; }
    .status-failed { color: #f85149; font-weight: bold; }
    
    /* Calendar Row */
    .cal-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 12px;
        border-bottom: 1px solid #30363d;
        background-color: #0d1117;
    }
    .cal-row:hover { background-color: #161b22; }
    
    /* Earnings Card */
    .earn-card {
        border-left: 5px solid #2196f3;
        background-color: #161b22;
        padding: 12px;
        border-radius: 6px;
        margin-bottom: 12px;
        border: 1px solid #30363d;
        border-left-width: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- DATABASE & MANAGER INIT ---
@st.cache_resource
def get_managers():
    infisical = InfisicalManager()
    if not infisical.is_connected:
        return None, None, None, None
    
    db_url, db_token = infisical.get_turso_news_credentials()
    a_url, a_token = infisical.get_turso_analyst_credentials()
    
    db = NewsDatabase(db_url, db_token) if db_url else None
    analyst_db = NewsDatabase(a_url, a_token, init_schema=False) if a_url else None
    pm = ScanProgressManager()
    
    return db, analyst_db, infisical, pm

db, analyst_db, infisical, scan_pm = get_managers()

# --- HELPER FUNCTIONS ---
def update_log(message):
    if "logs" not in st.session_state:
        st.session_state.logs = []
    st.session_state.logs.append(message)
    if "log_placeholder" in st.session_state:
        st.session_state.log_placeholder.code("\n".join(st.session_state.logs), language="bash")

@st.cache_data(ttl=300)
def fetch_news(target_date):
    if not db: return []
    return db.fetch_news_by_date(target_date)

@st.cache_data(ttl=3600)
def fetch_hunt_logs():
    if not db: return []
    try:
        sql = "SELECT * FROM hunt_logs ORDER BY started_at DESC LIMIT 15"
        rs = db.client.execute(sql)
        logs = []
        for row in rs.rows:
            logs.append({
                "id": row[0], "run": row[1], "date": row[2], "started": row[3],
                "ended": row[4], "status": row[5], "found": row[6], "total": row[7],
                "duration": row[8], "errors": row[9]
            })
        return logs
    except:
        return []

# --- MAIN UI ---
def main():
    if not db:
        st.error("❌ Database connection failed. Please check Infisical credentials.")
        return

    st.title("🦅 Grandmaster Hunt Dashboard")
    
    # 🕒 Top Indicators
    col_info1, col_info2, col_info3 = st.columns(3)
    last_update = db.get_last_update_time()
    available_tickers = analyst_db.fetch_monitored_tickers() if analyst_db else []
    
    with col_info1: st.success("✅ News DB Online")
    with col_info2: 
        if last_update:
            try:
                dt = dt_parser.parse(last_update)
                st.info(f"🕒 Last article: {dt.strftime('%H:%M %d-%b UTC')}")
            except: st.info(f"Last Update: {last_update}")
        else: st.info("No Data in DB")
    with col_info3: st.success(f"📈 {len(available_tickers)} Tickers Monitored")

    # --- TOP FORM (Date & Schedule) ---
    st.divider()
    
    col_date, col_sched = st.columns([1, 2])
    
    with col_date:
        st.subheader("📅 Target Date")
        # Snap to latest logical trading day
        default_date = market_utils.MarketCalendar.get_current_or_prev_trading_day(datetime.datetime.now().date())
        selected_date = st.date_input("Target Date", value=default_date, label_visibility="collapsed")
    
    with col_sched:
        st.subheader("📆 Weekly Schedule")
        start_of_week = selected_date - datetime.timedelta(days=selected_date.weekday())
        end_of_week = start_of_week + datetime.timedelta(days=6)
        
        with st.expander(f"Week of {start_of_week}", expanded=False):
            if st.button("🔄 Sync Weekly Schedule", use_container_width=True):
                with st.spinner("Scraping Yahoo & InvestPy..."):
                    cal_pop = CalendarPopulator(db, analyst_db)
                    count = cal_pop.sync_week(selected_date)
                    st.toast(f"✅ Synced {count} events!")
                    st.rerun()
            
            events = db.get_upcoming_events(start_of_week.isoformat(), end_of_week.isoformat())
            if not events:
                st.info("No events found. Sync to populate.")
            else:
                eco_data = [e for e in events if e.get('type') in ['MACRO_EVENT', 'MACRO']]
                earn_data = [e for e in events if e.get('type') == 'EARNINGS']
                cal_t1, cal_t2 = st.tabs(["📊 Economics", "🔔 Earnings"])
                
                with cal_t1:
                    df_eco = pd.DataFrame(eco_data)
                    if df_eco.empty:
                        st.info("No economic events for this week.")
                    else:
                        for d in sorted(df_eco['date'].unique()):
                            st.caption(f"📅 {d}")
                            for _, row in df_eco[df_eco['date'] == d].iterrows():
                                # Flag logic
                                cntry = row.get('country', 'US')
                                flag = "🇺🇸" if cntry == "US" else "🇬🇧" if cntry == "GB" else "🇪🇺" if cntry == "EU" else "🌍"
                                imp = row.get('importance', 'LOW')
                                imp_color = "red" if imp == "HIGH" else "orange" if imp == "MEDIUM" else "gray"
                                
                                st.markdown(f"""
                                <div class="cal-row">
                                    <div>{flag} <b>{row['name']}</b><br><small>🕒 {row.get('time', 'TBA')}</small></div>
                                    <div style="background:{imp_color}; color:white; padding:2px 8px; border-radius:4px; font-size:0.75rem;">{imp}</div>
                                </div>
                                """, unsafe_allow_html=True)
                
                with cal_t2:
                    df_earn = pd.DataFrame(earn_data)
                    if df_earn.empty:
                        st.info("No earnings reports for this week.")
                    else:
                        for d in sorted(df_earn['date'].unique()):
                            st.caption(f"📅 {d}")
                            e_cols = st.columns(3)
                            for i, row in df_earn[df_earn['date'] == d].reset_index().iterrows():
                                ticker = row.get('ticker', '?')
                                is_mega = ticker in MEGA_CAP_TICKERS
                                border = "#ff4b4b" if is_mega else "#2196f3"
                                
                                with e_cols[i % 3]:
                                    st.markdown(f"""
                                    <div class="earn-card" style="border-left-color:{border}">
                                        <div style="font-weight:bold; color:white;">{'🔥 ' if is_mega else ''}{ticker}</div>
                                        <div style="font-size:0.8rem; color:#bbb;">{row['name']}</div>
                                        <div style="font-size:0.75rem; color:#888;">🕒 {row.get('time', 'TBA')}</div>
                                        <div style="margin-top:5px; font-size:0.8rem; background:#262730; padding:4px; border-radius:4px;">
                                            Est: <b>{row.get('eps_estimate','-')}</b> | Act: <b>{row.get('eps_reported','-')}</b>
                                        </div>
                                    </div>
                                    """, unsafe_allow_html=True)

    # --- RESUME CAPABILITY ---
    resume_info = scan_pm.get_resume_info()
    if resume_info:
        st.warning(f"🛑 RECOVERY: Previous {resume_info['type']} scan was interrupted.")
        res_col1, res_col2 = st.columns(2)
        with res_col1:
            if st.button(f"▶️ Resume {resume_info['type']} ({len(resume_info['remaining'])} left)", type="primary"):
                st.session_state.doing_resume = True
                st.session_state.resume_info = resume_info
        with res_col2:
            if st.button("🗑️ Discard Progress"):
                scan_pm.clear_state()
                st.rerun()

    # --- HUNT CONTROL FORM ---
    st.divider()
    with st.form("grandmaster_hunt_form"):
        st.subheader("🚀 Initiate Grandmaster Hunt")
        c1, c2, c3 = st.columns(3)
        
        with c1:
            st.markdown("### 🌍 Macro")
            enable_macro = st.checkbox("Enable Macro Scan", value=True)
            macro_opts = [t['name'] for t in macro_engine.MACRO_RSS_TARGETS]
            selected_macro = st.multiselect("Topics", macro_opts, default=macro_opts, disabled=not enable_macro)
            
            # Event Injection
            evt_start = selected_date.strftime("%Y-%m-%d")
            raw_evts = db.get_upcoming_events(evt_start, evt_start)
            eco_evts = [e for e in raw_evts if e.get('type') != 'EARNINGS']
            sel_evts = []
            if eco_evts:
                sel_evts = st.multiselect("Inject Specific Events", [e['name'] for e in eco_evts], default=[e['name'] for e in eco_evts])
        
        with c2:
            st.markdown("### 📈 Stocks")
            enable_stocks = st.checkbox("Enable Stock Scan", value=True)
            stock_opts = [t['name'] for t in stocks_engine.YAHOO_RSS_TARGETS]
            selected_stocks = st.multiselect("Segments", stock_opts, default=stock_opts, disabled=not enable_stocks)
            
            # Earnings Injection
            earn_evts = [e['ticker'] for e in raw_evts if e.get('type') == 'EARNINGS']
            sel_earn = []
            if earn_evts:
                sel_earn = st.multiselect("Inject Earnings Hunt", earn_evts, default=[])
        
        with c3:
            st.markdown("### 🏢 Company")
            enable_company = st.checkbox("Enable Watchlist Scan", value=True)
            sel_watch = st.multiselect("Tickers", available_tickers, default=available_tickers, disabled=not enable_company)
        
        st.divider()
        ac1, ac2, ac3 = st.columns(3)
        with ac1: depth = st.number_input("Scan Depth (Pages)", 1, 10, 5)
        with ac2: force_fresh = st.checkbox("🔥 Force Fresh (No Cache)", False)
        with ac3: headless = st.toggle("👻 Headless Mode", True)
        
        launch_hunt = st.form_submit_button("🦅 START GRANDMASTER HUNT", type="primary", use_container_width=True)

    # --- EXECUTION LOG ---
    st.session_state.log_placeholder = st.empty()
    results_placeholder = st.container()

    # --- TRIGGER HUNT ---
    if launch_hunt or getattr(st.session_state, "doing_resume", False):
        st.session_state.logs = []
        is_resume = getattr(st.session_state, "doing_resume", False)
        r_info = getattr(st.session_state, "resume_info", None)
        
        update_log(f"🚀 INITIATING {'RESUME' if is_resume else 'GRANDMASTER'} HUNT PROTOCOL")
        update_log(f"🎯 TARGET DATE: {selected_date}")
        update_log(f"──────────────────────────────────────────────────")
        
        all_found = {}

        # 🚀 OPTIMIZATION: Load dedup context ONCE and reuse across all scan phases.
        cache = {} if force_fresh else db.fetch_cache_map(selected_date, None)
        titles = {} if force_fresh else db.fetch_existing_titles(selected_date)

        # 1. Macro Scan
        if enable_macro or (is_resume and r_info['type'] == 'MACRO'):
            update_log("🌍 Starting Macro Phase...")
            
            manual_feeds = None
            if sel_evts:
                chosen = [e for e in eco_evts if e['name'] in sel_evts]
                manual_feeds = macro_engine.build_feeds_from_events(chosen)
            
            m_res = macro_engine.run_macro_scan(
                selected_date, depth, update_log, db=db, cache_map=cache, 
                existing_titles=titles, target_subset=selected_macro,
                manual_event_feeds=manual_feeds, headless=headless,
                resume_targets=r_info['remaining'] if is_resume and r_info['type'] == 'MACRO' else None
            )
            for r in m_res.get('articles', []):
                cat = r.get('category', 'MACRO')
                if cat not in all_found: all_found[cat] = []
                all_found[cat].append(r)
                # Merge into shared dedup context
                norm_t = market_utils.normalize_title(r.get('title', '')).lower()
                titles[norm_t] = "macro_phase"
                if r.get('url'): cache[r['url']] = True

        # 2. Earnings Injection (MarketAux)
        if earn_evts and sel_earn:
            update_log(f"🕵️ Hunting Earnings for: {', '.join(sel_earn)}")
            ma_keys = infisical.get_marketaux_keys()
            if ma_keys:
                e_res = marketaux_engine.run_marketaux_scan(ma_keys, selected_date, sel_earn, update_log, db=db, cache_map=cache, existing_titles=titles, headless=headless)
                for r in e_res:
                    cat = "EARNINGS_HUNT"
                    if cat not in all_found: all_found[cat] = []
                    all_found[cat].append(r)

        # 3. Stock Scan (reuses shared dedup context — no DB reload)
        if enable_stocks or (is_resume and r_info['type'] == 'STOCKS'):
            update_log("📈 Starting Market Phase...")
            s_res = stocks_engine.run_stocks_scan(
                selected_date, depth, update_log, db=db, cache_map=cache,
                existing_titles=titles, target_subset=selected_stocks, headless=headless,
                resume_targets=r_info['remaining'] if is_resume and r_info['type'] == 'STOCKS' else None
            )
            for r in s_res.get('articles', []):
                cat = r.get('category', 'EQUITIES')
                if cat not in all_found: all_found[cat] = []
                all_found[cat].append(r)
                # Merge into shared dedup context
                norm_t = market_utils.normalize_title(r.get('title', '')).lower()
                titles[norm_t] = "stocks_phase"
                if r.get('url'): cache[r['url']] = True

        # 4. Company Scan (reuses shared dedup context)
        if enable_company or (is_resume and r_info['type'] == 'COMPANY'):
            update_log("🏢 Starting Watchlist Phase...")
            ma_keys = infisical.get_marketaux_keys()
            if ma_keys:
                c_res = marketaux_engine.run_marketaux_scan(
                    ma_keys, selected_date, sel_watch if not is_resume else r_info['remaining'], 
                    update_log, db=db, cache_map=cache, existing_titles=titles, headless=headless
                )
                for r in c_res:
                    cat = r.get('category', 'COMPANY')
                    if cat not in all_found: all_found[cat] = []
                    all_found[cat].append(r)

        update_log("🏁 MISSION ACCOMPLISHED.")
        st.session_state.doing_resume = False
        st.session_state.hunt_results = all_found

    # --- RESULTS DISPLAY ---
    results = getattr(st.session_state, "hunt_results", None)
    if results:
        with results_placeholder:
            st.divider()
            st.header(f"📊 Hunt Results ({selected_date})")
            
            # AI Export Text
            ai_text = f"--- NEWS EXPORT {selected_date} ---\n\n"
            
            res_tabs = st.tabs([f"📌 {cat}" for cat in results.keys()])
            for i, (cat, articles) in enumerate(results.items()):
                with res_tabs[i]:
                    ai_text += f"\n=== {cat} ===\n"
                    for art in articles:
                        ai_text += f"TITLE: {art['title']}\nTIME: {art.get('time','N/A')}\nURL: {art['url']}\nCONTENT: {art.get('content','')[:500]}...\n---\n"
                        with st.expander(f"{art.get('time','')} | {art['title']}", expanded=True):
                            st.caption(f"Source: {art.get('publisher','Unknown')} | {art.get('source_domain','')}")
                            st.write(f"[Link]({art['url']})")
                            snippet = art['content'] if isinstance(art['content'], str) else "\n".join(art['content'][:3])
                            st.write(snippet)
            
            st.divider()
            st.subheader("🧠 AI Export")
            st.download_button("📥 Download Everything for GPT/AI", data=ai_text, file_name=f"market_intel_{selected_date}.txt")

    # --- DASHBOARD FEED (Historical) ---
    st.divider()
    st.header(f"🛰️ Real-Time Intelligence Feed ({selected_date})")
    all_news = fetch_news(selected_date)
    
    if not all_news:
        st.info("No articles in DB for this date yet. Run a hunt above!")
    else:
        f_col1, f_col2, f_col3, f_col4 = st.columns(4)
        m_c = sum(1 for n in all_news if n.get('category') == 'MACRO')
        s_c = sum(1 for n in all_news if n.get('category') == 'STOCKS' or n.get('category') in stocks_engine.YAHOO_RSS_TARGETS)
        c_c = len(all_news) - m_c - s_c
        
        with f_col1: st.markdown(f'<div class="metric-card">🌍 Macro<br><h2>{m_c}</h2></div>', unsafe_allow_html=True)
        with f_col2: st.markdown(f'<div class="metric-card">📈 Stocks<br><h2>{s_c}</h2></div>', unsafe_allow_html=True)
        with f_col3: st.markdown(f'<div class="metric-card">🏢 Company<br><h2>{c_c}</h2></div>', unsafe_allow_html=True)
        with f_col4: st.markdown(f'<div class="metric-card">🗞️ Total<br><h2>{len(all_news)}</h2></div>', unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        feed_cat = st.multiselect("Filter Feed", ["MACRO", "STOCKS", "COMPANY", "EARNINGS_HUNT"], default=["MACRO", "STOCKS", "COMPANY", "EARNINGS_HUNT"])
        for item in all_news:
            if item.get('category') in feed_cat or (item.get('category') not in ["MACRO", "STOCKS"] and "COMPANY" in feed_cat):
                border = "#58a6ff" if item.get('category') == 'MACRO' else "#2ea043" if item.get('category') == 'STOCKS' else "#d29922"
                st.markdown(f"""
                <div class="news-card" style="border-left-color:{border}">
                    <a href="{item['url']}" target="_blank" class="news-title">{item['title']}</a>
                    <div class="news-meta"><b>{item.get('time','')}</b> | {item.get('publisher','')} | <i>{item.get('category','')}</i></div>
                    <div class="news-content">{" ".join(item.get('content', [])[:2])}...</div>
                </div>
                """, unsafe_allow_html=True)

    # --- HEARTBEAT LOGS ---
    st.divider()
    st.header("💓 Hunt Logs (Heartbeats)")
    logs = fetch_hunt_logs()
    if logs:
        for l in logs:
            with st.expander(f"Hunt Run {l['run']} - {l['date']} ({l['status']})"):
                st.write(f"Started: {l['started']} | Duration: {l['duration']:.1f}s")
                st.write(f"Found: {l['found']} | Total Session: {l['total']}")
                if l['errors']: st.error(f"Errors: {l['errors']}")

if __name__ == "__main__":
    main()
