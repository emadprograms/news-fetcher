# News Fetcher (Grandmaster Hunt)

A sophisticated news aggregation and analysis system designed for financial markets, featuring a Streamlit dashboard, Discord integration, and automated scraping engines.

## Tech Stack
- **Language:** Python 3.12+
- **Frontend:** Streamlit
- **Database:** Turso (libsql)
- **Secrets:** Infisical (infisicalsdk)
- **Automation:** Selenium (Headless Chrome), GitHub Actions
- **Data Sources:** Yahoo Finance, MarketAux, InvestPy, various RSS feeds
- **Messaging:** Discord (Webhooks & Bot)

## Project Structure
- `main.py`: Entry point for automated "Grandmaster Hunt" sessions.
- `streamlit_app.py`: Interactive dashboard for visualizing news and managing scans.
- `modules/`:
  - `engines/`: Scrapers for Macro, Stocks, and Company-specific news.
  - `clients/`: API clients for Database (Turso), Secrets (Infisical), and Calendars.
  - `utils/`: Market calendar logic and scan progress management.
- `discord_bot/`: Separate Discord bot service.
- `tools/`: Internal folder for scripts, experimentation, and debugging tools (e.g., `test_infisical.py`).
- `logs/`: Application and system logs.

## Architectural Decisions & Evolution

- **Lightweight Discord Bot:** To prevent server load and hanging, the Discord bot acts solely as a communication bridge. Commands like `!checkrawnews` and `!rawnews` do not query the database directly; they trigger GitHub Actions, which then perform the work and return results via Discord Webhooks.
- **Infisical SDK v3 Fixes:** Resolved `BaseSecret` attribute errors caused by the v3 SDK's nested secret structure. Implemented a robust `_extract_value` and `_extract_key_name` helper to handle both snake_case and camelCase attributes across SDK versions.
- **Dynamic Key Discovery:** MarketAux keys are discovered dynamically using a prefix search (`marketaux-` and `marketaux_`). The legacy `MARKETAUX_API_KEYS` list check was removed to eliminate unnecessary 404 errors during discovery.
- **Session-Aware Queries:** GitHub Actions and bot commands respect the `target_date` argument, ensuring queries for past sessions resolve to the correct historical windows.

## Engineering Standards
- **Tools Usage:** Always use the `tools/` folder for any new scripts, debugging, or investigative code. This keeps the root directory clean and provides a centralized place for project utilities.
- **Modular Design:** Keep scraping logic in `engines/` and infrastructure logic in `clients/`.

## Database Optimization (Turso Read Reduction)

The following patterns are critical for keeping Turso cloud read costs low. **All DB queries MUST follow these rules:**

### Query Rules
- **NEVER use `date(published_at)` in WHERE clauses.** The `date()` function prevents index usage and forces a full table scan. Always use range queries instead: `WHERE published_at >= ? AND published_at < ?` (next day).
- **NEVER use `LIKE ? || '%'` for date filtering.** Use range queries for the same reason.
- **NEVER call `article_exists()` inside per-item loops.** Use in-memory dedup (`seen_titles` dict + `seen_urls` set) instead. The `INSERT OR IGNORE` on the UNIQUE `url` column is the DB-level safety net.
- **`fetch_cache_map()` is lightweight.** It returns `{url: True}`, NOT full article dicts. Do not try to append cache_map values as article objects.

### Dedup Architecture
- **Load dedup context ONCE** at the start of a hunt session (`existing_titles` + `cache_map`), then pass both through all scan phases (Macro → Stocks → Company).
- **Merge new articles in-memory** after each phase: add newly found titles to `existing_titles` and URLs to `cache` so subsequent phases have fresh dedup without re-querying the database.
- **Engine fallback loading** (`fetch_existing_titles`) uses a single-date range query, NOT the old 3-day window with 3 separate queries.

### Indexes
- `idx_cat_date` on `(category, published_at)` — primary query index
- `idx_title` on `(title)` — dedup lookups (prevents full table scan on title checks)
- `idx_url` on `(url)` — existence checks

### Key Methods
- `article_exists(url, title)`: Combined single query using `OR` (1 round-trip instead of 2).
- `batch_urls_exist(urls)`: Bulk URL existence check using `IN (...)` in batches of 50.
- `fetch_cache_map(date)`: Returns `{url: True}` — only fetches URL column, not full content.
- `fetch_existing_titles(date)`: Returns `{normalized_title: id}` using range query.
- `fetch_existing_titles_range(start, end)`: Same but for session windows spanning multiple days.

## Common Commands
- **Run Dashboard:** `streamlit run streamlit_app.py`
- **Run Automation:** `python main.py`
- **Run Discord Bot:** `python discord_bot/bot.py`
- **Run Status Check (Manual):** `MODE=CHECK python main.py`
- **Tests:** `pytest`

## Discord Bot Commands
- `!rawnews [YYYY-MM-DD]`: Triggers the GitHub Actions workflow to run a full news hunt for the specified or current trading session.
- `!checkrawnews [YYYY-MM-DD]`: Triggers a lightweight GitHub Action that queries the database for the session window and article count, then delivers the result via webhook. Wrap links in `<>` to avoid Discord banners.

## Critical Files
- `.streamlit/secrets.toml`: Local Streamlit secrets (use Infisical for production).
- `requirements.txt`: Project dependencies.
- `modules/clients/infisical_client.py`: Core secret manager used across the app.
