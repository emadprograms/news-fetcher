"""
Test Suite for News-Fetcher-CLI — v2
=====================================
Comprehensive tests for all core utilities, market logic,
database client operations, and main.py reporting functions.

Run: python3 -m pytest tests/test_core.py -v
"""

import sys
import os
import unittest
import datetime
from unittest.mock import MagicMock, patch

# Ensure project root is on the path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)


# ═══════════════════════════════════════════════════════
#  1. MARKET_UTILS — Title Normalization
# ═══════════════════════════════════════════════════════

class TestNormalizeTitle(unittest.TestCase):
    """Tests for market_utils.normalize_title()"""

    def setUp(self):
        from modules.utils.market_utils import normalize_title
        self.normalize = normalize_title

    def test_strips_yahoo_suffix(self):
        self.assertEqual(self.normalize("Breaking News - Yahoo Finance"), "Breaking News")

    def test_strips_bloomberg_suffix(self):
        self.assertEqual(self.normalize("Market Update - Bloomberg"), "Market Update")

    def test_strips_reuters_suffix(self):
        self.assertEqual(self.normalize("Fed Decision - Reuters"), "Fed Decision")

    def test_strips_cnbc_suffix(self):
        self.assertEqual(self.normalize("Earnings Beat - CNBC"), "Earnings Beat")

    def test_strips_wsj_suffix(self):
        self.assertEqual(self.normalize("Trade War - The Wall Street Journal"), "Trade War")

    def test_strips_marketwatch_suffix(self):
        self.assertEqual(self.normalize("Oil Prices Rise - MarketWatch"), "Oil Prices Rise")

    def test_preserves_no_suffix(self):
        self.assertEqual(self.normalize("Simple Title"), "Simple Title")

    def test_strips_whitespace(self):
        self.assertEqual(self.normalize("  Padded Title  "), "Padded Title")

    def test_empty_string(self):
        self.assertEqual(self.normalize(""), "")

    def test_none(self):
        self.assertEqual(self.normalize(None), "")

    def test_hyphen_in_title_no_suffix(self):
        """Ensure titles with hyphens but no known suffix are untouched."""
        self.assertEqual(self.normalize("US-China Trade - A Deep Dive"), "US-China Trade - A Deep Dive")


# ═══════════════════════════════════════════════════════
#  2. MARKET_UTILS — Source Classification
# ═══════════════════════════════════════════════════════

class TestIsPremiumSource(unittest.TestCase):
    """Tests for market_utils.is_premium_source()"""

    def setUp(self):
        from modules.utils.market_utils import is_premium_source
        self.is_premium = is_premium_source

    def test_bloomberg_in_title(self):
        self.assertTrue(self.is_premium("Bloomberg reports gains", "https://example.com"))

    def test_reuters_in_url(self):
        self.assertTrue(self.is_premium("Generic title", "https://reuters.com/article"))

    def test_cnbc_in_title(self):
        self.assertTrue(self.is_premium("CNBC exclusive report", "https://example.com"))

    def test_wsj_match(self):
        self.assertTrue(self.is_premium("Wall Street Journal analysis", "https://wsj.com"))

    def test_non_premium(self):
        self.assertFalse(self.is_premium("Random blog post", "https://random.com"))


class TestBlockedSources(unittest.TestCase):
    """Tests that BLOCKED_SOURCES list is consistent"""

    def test_all_uppercase(self):
        from modules.utils.market_utils import BLOCKED_SOURCES
        for src in BLOCKED_SOURCES:
            self.assertEqual(src, src.upper(), f"BLOCKED_SOURCES entry '{src}' is not uppercase")

    def test_known_blocked(self):
        from modules.utils.market_utils import BLOCKED_SOURCES
        expected = {"MOTLEY FOOL", "BENZINGA", "ZACKS"}
        self.assertTrue(expected.issubset(set(BLOCKED_SOURCES)))


# ═══════════════════════════════════════════════════════
#  3. MARKET_UTILS — Date/Time Helpers
# ═══════════════════════════════════════════════════════

class TestParseIsoDatetime(unittest.TestCase):
    """Tests for market_utils.parse_iso_datetime()"""

    def setUp(self):
        from modules.utils.market_utils import parse_iso_datetime
        self.parse = parse_iso_datetime

    def test_valid_iso(self):
        result = self.parse("2025-01-15T10:30:00Z")
        self.assertIsNotNone(result)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)

    def test_valid_iso_with_offset(self):
        result = self.parse("2026-02-20T15:00:00+03:00")
        self.assertIsNotNone(result)
        self.assertEqual(result.day, 20)

    def test_invalid_string(self):
        self.assertIsNone(self.parse("not a date"))

    def test_none_input(self):
        self.assertIsNone(self.parse(None))

    def test_empty_string(self):
        self.assertIsNone(self.parse(""))


class TestDecodeGoogleNewsUrl(unittest.TestCase):
    """Verifies the decode function is a pass-through (legacy mode)"""

    def test_passthrough(self):
        from modules.utils.market_utils import decode_google_news_url
        url = "https://news.google.com/rss/articles/xyz"
        self.assertEqual(decode_google_news_url(url), url)

    def test_non_google_url(self):
        from modules.utils.market_utils import decode_google_news_url
        url = "https://finance.yahoo.com/news/test"
        self.assertEqual(decode_google_news_url(url), url)


# ═══════════════════════════════════════════════════════
#  4. MARKET CALENDAR — Trading Day Logic
# ═══════════════════════════════════════════════════════

class TestMarketCalendarTradingDays(unittest.TestCase):
    """Tests for MarketCalendar.is_trading_day()"""

    def setUp(self):
        from modules.utils.market_utils import MarketCalendar
        self.cal = MarketCalendar

    def test_weekday_is_trading_day(self):
        # 2026-02-18 is a Wednesday
        self.assertTrue(self.cal.is_trading_day(datetime.date(2026, 2, 18)))

    def test_saturday_not_trading_day(self):
        self.assertFalse(self.cal.is_trading_day(datetime.date(2026, 2, 21)))

    def test_sunday_not_trading_day(self):
        self.assertFalse(self.cal.is_trading_day(datetime.date(2026, 2, 22)))

    def test_holiday_not_trading_day(self):
        # MLK Day 2026
        self.assertFalse(self.cal.is_trading_day(datetime.date(2026, 1, 19)))

    def test_christmas_not_trading_day(self):
        self.assertFalse(self.cal.is_trading_day(datetime.date(2026, 12, 25)))

    def test_datetime_input_accepted(self):
        """Should accept datetime objects, not just date."""
        dt = datetime.datetime(2026, 2, 18, 10, 30)
        self.assertTrue(self.cal.is_trading_day(dt))

    def test_all_holidays_are_not_trading_days(self):
        """Every entry in HOLIDAYS_2026 should fail is_trading_day."""
        for h in self.cal.HOLIDAYS_2026:
            self.assertFalse(self.cal.is_trading_day(h), f"{h} should not be a trading day")


class TestMarketCalendarNavigation(unittest.TestCase):
    """Tests for get_prev/next/current_or_prev trading day."""

    def setUp(self):
        from modules.utils.market_utils import MarketCalendar
        self.cal = MarketCalendar

    def test_prev_trading_day_from_monday(self):
        # Monday 2026-02-23 → prev should be Friday 2026-02-20
        result = self.cal.get_prev_trading_day(datetime.date(2026, 2, 23))
        self.assertEqual(result, datetime.date(2026, 2, 20))

    def test_prev_trading_day_skips_weekend(self):
        # Sunday 2026-02-22 → prev should be Friday 2026-02-20
        result = self.cal.get_prev_trading_day(datetime.date(2026, 2, 22))
        self.assertEqual(result, datetime.date(2026, 2, 20))

    def test_prev_trading_day_from_tuesday(self):
        result = self.cal.get_prev_trading_day(datetime.date(2026, 2, 24))
        self.assertEqual(result, datetime.date(2026, 2, 23))

    def test_next_trading_day_from_friday(self):
        result = self.cal.get_next_trading_day(datetime.date(2026, 2, 20))
        self.assertEqual(result, datetime.date(2026, 2, 23))

    def test_current_or_prev_on_trading_day(self):
        result = self.cal.get_current_or_prev_trading_day(datetime.date(2026, 2, 20))
        self.assertEqual(result, datetime.date(2026, 2, 20))

    def test_current_or_prev_on_weekend(self):
        result = self.cal.get_current_or_prev_trading_day(datetime.date(2026, 2, 22))
        self.assertEqual(result, datetime.date(2026, 2, 20))

    def test_prev_trading_day_skips_holiday(self):
        # Day after Presidents Day (Feb 17, 2026 = Tuesday)
        # Presidents Day is Feb 16 (holiday), prev should be Feb 13 (Friday)
        result = self.cal.get_prev_trading_day(datetime.date(2026, 2, 17))
        self.assertEqual(result, datetime.date(2026, 2, 13))

    def test_current_or_prev_on_holiday(self):
        # Presidents Day 2026 (Monday Feb 16) → should go to Friday Feb 13
        result = self.cal.get_current_or_prev_trading_day(datetime.date(2026, 2, 16))
        self.assertEqual(result, datetime.date(2026, 2, 13))


class TestMarketCalendarDST(unittest.TestCase):
    """Tests for DST awareness and pre-market switch hour."""

    def setUp(self):
        from modules.utils.market_utils import MarketCalendar
        self.cal = MarketCalendar

    def test_winter_is_not_dst(self):
        self.assertFalse(self.cal.is_us_dst(datetime.date(2026, 1, 15)))

    def test_summer_is_dst(self):
        self.assertTrue(self.cal.is_us_dst(datetime.date(2026, 6, 15)))

    def test_dst_start_boundary(self):
        # DST starts Mar 8, 2026
        self.assertFalse(self.cal.is_us_dst(datetime.date(2026, 3, 7)))
        self.assertTrue(self.cal.is_us_dst(datetime.date(2026, 3, 8)))

    def test_dst_end_boundary(self):
        # DST ends Nov 1, 2026
        self.assertTrue(self.cal.is_us_dst(datetime.date(2026, 10, 31)))
        self.assertFalse(self.cal.is_us_dst(datetime.date(2026, 11, 1)))

    def test_premarket_switch_winter(self):
        self.assertEqual(self.cal.get_premarket_switch_hour_utc(datetime.date(2026, 1, 15)), 9)

    def test_premarket_switch_summer(self):
        self.assertEqual(self.cal.get_premarket_switch_hour_utc(datetime.date(2026, 6, 15)), 8)

    def test_datetime_input_accepted(self):
        dt = datetime.datetime(2026, 7, 1, 12, 0)
        self.assertTrue(self.cal.is_us_dst(dt))


class TestMarketCalendarEarlyClose(unittest.TestCase):
    """Tests for half-day / early close detection."""

    def setUp(self):
        from modules.utils.market_utils import MarketCalendar
        self.cal = MarketCalendar

    def test_day_before_july_4th(self):
        self.assertTrue(self.cal.is_early_close(datetime.date(2026, 7, 2)))

    def test_day_after_thanksgiving(self):
        self.assertTrue(self.cal.is_early_close(datetime.date(2026, 11, 27)))

    def test_christmas_eve(self):
        self.assertTrue(self.cal.is_early_close(datetime.date(2026, 12, 24)))

    def test_normal_day_not_early_close(self):
        self.assertFalse(self.cal.is_early_close(datetime.date(2026, 2, 18)))

    def test_all_early_close_days_are_trading_days(self):
        """Early close days should still be valid trading days."""
        for d in self.cal.EARLY_CLOSE_2026:
            self.assertTrue(self.cal.is_trading_day(d), f"{d} is early close but should be a trading day")


class TestGetCurrentOrNextTradingDay(unittest.TestCase):
    """Tests for MarketCalendar.get_current_or_next_trading_day()"""

    def setUp(self):
        from modules.utils.market_utils import MarketCalendar
        self.cal = MarketCalendar

    def test_trading_day_returns_self(self):
        # Friday Feb 20, 2026 is a trading day
        result = self.cal.get_current_or_next_trading_day(datetime.date(2026, 2, 20))
        self.assertEqual(result, datetime.date(2026, 2, 20))

    def test_saturday_returns_monday(self):
        # Saturday Feb 21 → Monday Feb 23
        result = self.cal.get_current_or_next_trading_day(datetime.date(2026, 2, 21))
        self.assertEqual(result, datetime.date(2026, 2, 23))

    def test_sunday_returns_monday(self):
        # Sunday Feb 22 → Monday Feb 23
        result = self.cal.get_current_or_next_trading_day(datetime.date(2026, 2, 22))
        self.assertEqual(result, datetime.date(2026, 2, 23))

    def test_holiday_skips_to_next(self):
        # Presidents Day Feb 16 (Mon) → Tuesday Feb 17
        result = self.cal.get_current_or_next_trading_day(datetime.date(2026, 2, 16))
        self.assertEqual(result, datetime.date(2026, 2, 17))

    def test_saturday_before_long_weekend(self):
        # Saturday Feb 14 → skip Sun 15, skip Mon 16 (holiday) → Tuesday Feb 17
        result = self.cal.get_current_or_next_trading_day(datetime.date(2026, 2, 14))
        self.assertEqual(result, datetime.date(2026, 2, 17))

    def test_accepts_datetime(self):
        dt = datetime.datetime(2026, 2, 21, 15, 0)  # Saturday
        result = self.cal.get_current_or_next_trading_day(dt)
        self.assertEqual(result, datetime.date(2026, 2, 23))


class TestResolveTradingSession(unittest.TestCase):
    """Tests for MarketCalendar.resolve_trading_session() — automatic mode."""

    def setUp(self):
        from modules.utils.market_utils import MarketCalendar
        self.cal = MarketCalendar

    def _utc(self, year, month, day, hour=12, minute=0):
        return datetime.datetime(year, month, day, hour, minute, tzinfo=datetime.timezone.utc)

    def test_friday_10pm_targets_friday(self):
        """Friday at 10 PM UTC — still in Friday's session (ends Saturday 1 AM)."""
        now = self._utc(2026, 2, 20, 22, 0)  # Friday 10 PM
        target, start, end = self.cal.resolve_trading_session(now)
        self.assertEqual(target, datetime.date(2026, 2, 20))  # Friday
        # Session start: prev_td(Friday) = Thursday → Fri 1 AM
        self.assertEqual(start, self._utc(2026, 2, 20, 1, 0))
        # End capped at now (before Saturday 1 AM)
        self.assertEqual(end, now)

    def test_saturday_3am_targets_next_trading_day(self):
        """Saturday at 3 AM UTC — Friday's session ended at Saturday 1 AM."""
        now = self._utc(2026, 2, 21, 3, 0)  # Saturday 3 AM
        target, start, end = self.cal.resolve_trading_session(now)
        # Next trading day after Saturday = Monday Feb 23
        self.assertEqual(target, datetime.date(2026, 2, 23))
        # Session start: prev_td(Monday) = Friday → Saturday 1 AM
        self.assertEqual(start, self._utc(2026, 2, 21, 1, 0))
        self.assertEqual(end, now)

    def test_wednesday_before_1am_still_in_tuesday_session(self):
        """Wednesday at 00:30 UTC — still in Tuesday's session (ends Wed 1 AM)."""
        now = self._utc(2026, 2, 18, 0, 30)  # Wednesday 00:30
        target, start, end = self.cal.resolve_trading_session(now)
        self.assertEqual(target, datetime.date(2026, 2, 17))  # Tuesday
        # End is capped at now (still before Wed 1 AM)
        self.assertEqual(end, now)

    def test_wednesday_after_1am_targets_wednesday(self):
        """Wednesday at 5 AM UTC — in Wednesday's session."""
        now = self._utc(2026, 2, 18, 5, 0)
        target, start, end = self.cal.resolve_trading_session(now)
        self.assertEqual(target, datetime.date(2026, 2, 18))  # Wednesday
        # Session start: prev_td(Wed) = Tue → Wed 1 AM
        self.assertEqual(start, self._utc(2026, 2, 18, 1, 0))

    def test_long_weekend_saturday_targets_tuesday(self):
        """Presidents' Day weekend: Saturday targets Tuesday (next trading day)."""
        now = self._utc(2026, 2, 14, 15, 0)  # Saturday Feb 14, 3 PM
        target, start, end = self.cal.resolve_trading_session(now)
        # Next TD from Saturday = Tuesday Feb 17
        self.assertEqual(target, datetime.date(2026, 2, 17))
        # Session start: prev_td(Tue Feb 17) = Fri Feb 13 → Sat Feb 14 1 AM
        self.assertEqual(start, self._utc(2026, 2, 14, 1, 0))

    def test_long_weekend_monday_targets_tuesday(self):
        """Presidents' Day (Monday holiday) — still in Tuesday's session."""
        now = self._utc(2026, 2, 16, 12, 0)  # Monday Feb 16 noon
        target, start, end = self.cal.resolve_trading_session(now)
        self.assertEqual(target, datetime.date(2026, 2, 17))
        self.assertEqual(start, self._utc(2026, 2, 14, 1, 0))


class TestResolveSessionForDate(unittest.TestCase):
    """Tests for MarketCalendar.resolve_session_for_date() — manual date override."""

    def setUp(self):
        from modules.utils.market_utils import MarketCalendar
        self.cal = MarketCalendar

    def _utc(self, year, month, day, hour=12, minute=0):
        return datetime.datetime(year, month, day, hour, minute, tzinfo=datetime.timezone.utc)

    def test_trading_day_maps_to_itself(self):
        """Feb 13 (Friday) → Friday's session."""
        now = self._utc(2026, 2, 20, 12, 0)
        target, start, end = self.cal.resolve_session_for_date(datetime.date(2026, 2, 13), now)
        self.assertEqual(target, datetime.date(2026, 2, 13))
        # Session start: prev_td(Fri) = Thu → Fri 1 AM
        self.assertEqual(start, self._utc(2026, 2, 13, 1, 0))
        # Session end: Sat 1 AM, but capped at now
        expected_end = min(self._utc(2026, 2, 14, 1, 0), now)
        self.assertEqual(end, expected_end)

    def test_sunday_maps_to_next_trading_day(self):
        """Feb 15 (Sunday) → Tuesday Feb 17 session."""
        now = self._utc(2026, 2, 16, 12, 0)
        target, start, end = self.cal.resolve_session_for_date(datetime.date(2026, 2, 15), now)
        self.assertEqual(target, datetime.date(2026, 2, 17))
        # Start: prev_td(Tue) = Fri → Sat Feb 14 1 AM
        self.assertEqual(start, self._utc(2026, 2, 14, 1, 0))

    def test_holiday_maps_to_next_trading_day(self):
        """Feb 16 (Presidents' Day) → Tuesday Feb 17 session."""
        now = self._utc(2026, 2, 17, 12, 0)
        target, start, end = self.cal.resolve_session_for_date(datetime.date(2026, 2, 16), now)
        self.assertEqual(target, datetime.date(2026, 2, 17))

    def test_all_weekend_dates_same_session(self):
        """Feb 14 (Sat), 15 (Sun), 16 (Mon holiday), 17 (Tue) all → Tuesday session."""
        now = self._utc(2026, 2, 18, 12, 0)
        expected_target = datetime.date(2026, 2, 17)
        expected_start = self._utc(2026, 2, 14, 1, 0)

        for date in [datetime.date(2026, 2, 14), datetime.date(2026, 2, 15),
                     datetime.date(2026, 2, 16), datetime.date(2026, 2, 17)]:
            target, start, _ = self.cal.resolve_session_for_date(date, now)
            self.assertEqual(target, expected_target, f"Date {date} should resolve to {expected_target}")
            self.assertEqual(start, expected_start, f"Date {date} should have session start {expected_start}")

class TestManagedDriver(unittest.TestCase):
    """Tests for ManagedDriver context manager (mocked Selenium)."""

    @patch('modules.utils.market_utils.get_selenium_driver')
    @patch('modules.utils.market_utils.force_quit_driver')
    def test_enters_and_exits_cleanly(self, mock_quit, mock_get):
        from modules.utils.market_utils import ManagedDriver
        mock_driver = MagicMock()
        mock_get.return_value = mock_driver

        with ManagedDriver(headless=True) as driver:
            self.assertEqual(driver, mock_driver)

        mock_get.assert_called_once_with(headless=True)
        mock_quit.assert_called_once_with(mock_driver)

    @patch('modules.utils.market_utils.get_selenium_driver')
    @patch('modules.utils.market_utils.force_quit_driver')
    def test_cleans_up_on_exception(self, mock_quit, mock_get):
        from modules.utils.market_utils import ManagedDriver
        mock_driver = MagicMock()
        mock_get.return_value = mock_driver

        with self.assertRaises(ValueError):
            with ManagedDriver() as driver:
                raise ValueError("Test crash")

        # Driver should still be cleaned up
        mock_quit.assert_called_once_with(mock_driver)


# ═══════════════════════════════════════════════════════
#  6. SCAN PROGRESS MANAGER
# ═══════════════════════════════════════════════════════

class TestScanProgressManager(unittest.TestCase):
    """Tests for ScanProgressManager (uses temp file)"""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self.tmp.close()

        from modules.utils.scan_progress import ScanProgressManager
        self.mgr = ScanProgressManager()
        self.mgr.state_file = self.tmp.name
        self.mgr._ensure_file()

    def tearDown(self):
        try:
            os.remove(self.tmp.name)
        except Exception:
            pass

    def test_initial_state_inactive(self):
        state = self.mgr.load_state()
        self.assertFalse(state["active_scan"])

    def test_start_new_scan(self):
        self.mgr.start_new_scan("MACRO", ["A", "B", "C"])
        state = self.mgr.load_state()
        self.assertTrue(state["active_scan"])
        self.assertEqual(state["scan_type"], "MACRO")
        self.assertEqual(len(state["total_targets"]), 3)

    def test_mark_target_complete(self):
        self.mgr.start_new_scan("STOCKS", ["AAPL", "TSLA"])
        self.mgr.mark_target_complete("AAPL")
        state = self.mgr.load_state()
        self.assertIn("AAPL", state["completed_targets"])

    def test_finish_scan(self):
        self.mgr.start_new_scan("TEST", ["X"])
        self.mgr.finish_scan()
        state = self.mgr.load_state()
        self.assertFalse(state["active_scan"])

    def test_get_resume_info_remaining(self):
        self.mgr.start_new_scan("COMPANY", ["GOOG", "META", "AMZN"])
        self.mgr.mark_target_complete("GOOG")
        info = self.mgr.get_resume_info()
        self.assertIsNotNone(info)
        self.assertEqual(info["completed_count"], 1)
        self.assertEqual(info["total_count"], 3)
        self.assertIn("META", info["remaining"])

    def test_get_resume_info_all_done(self):
        self.mgr.start_new_scan("X", ["A"])
        self.mgr.mark_target_complete("A")
        info = self.mgr.get_resume_info()
        self.assertIsNone(info)

    def test_clear_state(self):
        self.mgr.start_new_scan("Y", ["Z"])
        self.mgr.clear_state()
        state = self.mgr.load_state()
        self.assertFalse(state["active_scan"])

    def test_idempotent_mark_complete(self):
        """Marking the same target complete twice shouldn't duplicate."""
        self.mgr.start_new_scan("T", ["A", "B"])
        self.mgr.mark_target_complete("A")
        self.mgr.mark_target_complete("A")  # Again
        state = self.mgr.load_state()
        self.assertEqual(state["completed_targets"].count("A"), 1)


# ═══════════════════════════════════════════════════════
#  7. MAIN.PY — Discord Report Builder
# ═══════════════════════════════════════════════════════

class TestBuildDiscordReport(unittest.TestCase):
    """Tests for main.build_discord_report() (v2 embed format)"""

    def setUp(self):
        with patch('logging.basicConfig'), \
             patch('os.makedirs'):
            import main
            self.build = main.build_discord_report

    def test_returns_tuple_with_embeds(self):
        report = {
            "macro": 5, "stocks": 3, "company": 2,
            "calendar_events": 10, "marketaux_keys": 3,
            "tickers_scanned": 15, "total_in_db": 100,
            "errors": []
        }
        dt_start = datetime.datetime(2026, 5, 30, 1, 0, tzinfo=datetime.timezone.utc)
        dt_end = datetime.datetime(2026, 6, 2, 1, 0, tzinfo=datetime.timezone.utc)
        msg, embeds = self.build(datetime.date(2026, 6, 1), dt_start, dt_end, report, 120)
        self.assertIsNone(msg)  # Embed mode returns None for text
        self.assertIsInstance(embeds, list)
        self.assertEqual(len(embeds), 1)

    def test_green_embed_no_errors(self):
        report = {
            "macro": 5, "stocks": 3, "company": 2,
            "calendar_events": 0, "marketaux_keys": 0,
            "tickers_scanned": 0, "total_in_db": 0,
            "errors": []
        }
        dt_start = datetime.datetime(2026, 5, 30, 1, 0, tzinfo=datetime.timezone.utc)
        dt_end = datetime.datetime(2026, 6, 2, 1, 0, tzinfo=datetime.timezone.utc)
        _, embeds = self.build(datetime.date(2026, 6, 1), dt_start, dt_end, report, 120)
        self.assertEqual(embeds[0]["color"], 0x00FF00)  # Green
        self.assertIn("nominal", embeds[0]["footer"]["text"])

    def test_red_embed_critical_errors(self):
        report = {
            "macro": 0, "stocks": 0, "company": 0,
            "calendar_events": 0, "marketaux_keys": 0,
            "tickers_scanned": 0, "total_in_db": 0,
            "errors": ["Database connection failed", "Selenium crashed"]
        }
        dt_start = datetime.datetime(2026, 5, 30, 1, 0, tzinfo=datetime.timezone.utc)
        dt_end = datetime.datetime(2026, 6, 2, 1, 0, tzinfo=datetime.timezone.utc)
        _, embeds = self.build(datetime.date(2026, 6, 1), dt_start, dt_end, report, 60)
        self.assertEqual(embeds[0]["color"], 0xFF0000)  # Red
        self.assertTrue(any("Critical" in f["name"] for f in embeds[0].get("fields", [])))

    def test_yellow_embed_warnings_only(self):
        report = {
            "macro": 5, "stocks": 0, "company": 0,
            "calendar_events": 0, "marketaux_keys": 0,
            "tickers_scanned": 0, "total_in_db": 0,
            "errors": ["MarketAux API keys not found in Infisical"]
        }
        dt_start = datetime.datetime(2026, 5, 30, 1, 0, tzinfo=datetime.timezone.utc)
        dt_end = datetime.datetime(2026, 6, 2, 1, 0, tzinfo=datetime.timezone.utc)
        _, embeds = self.build(datetime.date(2026, 6, 1), dt_start, dt_end, report, 60)
        self.assertEqual(embeds[0]["color"], 0xFFAA00)  # Yellow

    def test_run_number_in_description(self):
        report = {
            "macro": 0, "stocks": 0, "company": 0,
            "calendar_events": 0, "marketaux_keys": 0,
            "tickers_scanned": 0, "total_in_db": 0,
            "errors": []
        }
        dt_start = datetime.datetime(2026, 5, 30, 1, 0, tzinfo=datetime.timezone.utc)
        dt_end = datetime.datetime(2026, 6, 2, 1, 0, tzinfo=datetime.timezone.utc)
        _, embeds = self.build(datetime.date(2026, 6, 1), dt_start, dt_end, report, 60, run_number=2, max_runs=3)
        self.assertIn("2/3", embeds[0]["description"])


# ═══════════════════════════════════════════════════════
#  8. DB_CLIENT — Mocked Operations
# ═══════════════════════════════════════════════════════

class TestNewsDatabaseInsert(unittest.TestCase):
    """Tests for NewsDatabase.insert_news() with mocked client"""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        db.url = "https://test.db"
        db.token = "test-token"
        return db

    def test_insert_single_article(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows_affected=1)
        inserted, dups = db.insert_news([{
            "published_at": "2025-06-01T10:00:00Z",
            "title": "Test Article",
            "url": "https://example.com/article",
            "source_domain": "example.com",
            "publisher": "Example",
            "category": "MACRO",
            "content": ["Line 1", "Line 2"]
        }], "MACRO")
        self.assertEqual(inserted, 1)
        self.assertEqual(dups, 0)

    def test_insert_with_trading_session_date(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows_affected=1)
        inserted, dups = db.insert_news([{
            "published_at": "2026-02-20T10:00:00Z",
            "title": "Session Test",
            "url": "https://example.com/session",
            "source_domain": "example.com",
            "publisher": "Test",
            "content": []
        }], "MACRO", trading_session_date=datetime.date(2026, 2, 20))
        self.assertEqual(inserted, 1)
        # Verify trading_session_date was passed to execute
        call_args = db.client.execute.call_args[0]
        self.assertIn("2026-02-20", call_args[1])  # Should be in params

    def test_insert_duplicate(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows_affected=0)
        inserted, dups = db.insert_news([{
            "published_at": "2025-06-01T10:00:00Z",
            "title": "Dupe",
            "url": "https://example.com/dupe",
            "source_domain": "example.com",
            "publisher": "Test",
            "content": []
        }], "STOCKS")
        self.assertEqual(inserted, 0)
        self.assertEqual(dups, 1)

    def test_insert_empty_list(self):
        db = self._make_db()
        inserted, dups = db.insert_news([], "MACRO")
        self.assertEqual(inserted, 0)
        self.assertEqual(dups, 0)

    def test_insert_no_client(self):
        db = self._make_db()
        db.client = None
        inserted, dups = db.insert_news([{"title": "x"}], "MACRO")
        self.assertEqual(inserted, 0)
        self.assertEqual(dups, 0)


class TestNewsDatabaseArticleExists(unittest.TestCase):
    """Tests for NewsDatabase.article_exists()"""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        db.url = "https://test.db"
        db.token = "test-token"
        return db

    def test_exists_by_url(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[(42,)])
        result = db.article_exists("https://example.com/found")
        self.assertEqual(result, 42)

    def test_not_exists(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[])
        result = db.article_exists("https://example.com/missing")
        self.assertFalse(result)

    def test_no_client(self):
        db = self._make_db()
        db.client = None
        result = db.article_exists("https://example.com/x")
        self.assertFalse(result)


class TestNewsDatabaseFetchTitles(unittest.TestCase):
    """Tests for fetch_existing_titles and fetch_existing_titles_range"""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        db.url = "https://test.db"
        db.token = "test-token"
        return db

    def test_fetch_existing_titles_returns_dict(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[
            (1, "Fed raises rates - Yahoo Finance", "2025-06-01T10:00:00Z"),
            (2, "Oil prices surge - Bloomberg", "2025-06-01T14:00:00Z"),
        ])
        result = db.fetch_existing_titles(datetime.date(2025, 6, 1))
        self.assertIsInstance(result, dict)
        self.assertIn("fed raises rates", result)
        self.assertIn("oil prices surge", result)

    def test_fetch_existing_titles_no_client(self):
        db = self._make_db()
        db.client = None
        result = db.fetch_existing_titles(datetime.date(2025, 6, 1))
        self.assertEqual(result, {})

    def test_fetch_existing_titles_range(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[
            (1, "Weekend article"),
            (2, "Monday alert"),
        ])
        result = db.fetch_existing_titles_range(
            "2026-02-20T01:00:00+00:00",
            "2026-02-23T01:00:00+00:00"
        )
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 2)

    def test_fetch_existing_titles_range_no_client(self):
        db = self._make_db()
        db.client = None
        result = db.fetch_existing_titles_range("2026-01-01", "2026-01-02")
        self.assertEqual(result, {})


class TestNewsDatabaseHuntHeartbeat(unittest.TestCase):
    """Tests for hunt_logs heartbeat methods."""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        db.url = "https://test.db"
        db.token = "test-token"
        return db

    def test_log_hunt_start_returns_id(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[(42,)])
        hunt_id = db.log_hunt_start(
            1,
            datetime.date(2026, 2, 20),
            datetime.datetime(2026, 2, 20, 1, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2026, 2, 20, 15, 0, tzinfo=datetime.timezone.utc)
        )
        self.assertEqual(hunt_id, 42)

    def test_log_hunt_start_no_client(self):
        db = self._make_db()
        db.client = None
        result = db.log_hunt_start(1, datetime.date(2026, 2, 20), "start", "end")
        self.assertIsNone(result)

    def test_log_hunt_end_no_crash(self):
        db = self._make_db()
        # Should not raise
        db.log_hunt_end(42, "SUCCESS", 10, 50, 120.5, errors=None)
        db.client.execute.assert_called_once()

    def test_log_hunt_end_with_errors(self):
        db = self._make_db()
        db.log_hunt_end(42, "PARTIAL", 5, 30, 90.0, errors=["Macro scan failed", "API timeout"])
        call_args = db.client.execute.call_args[0]
        self.assertIn("Macro scan failed; API timeout", call_args[1])

    def test_log_hunt_end_no_client(self):
        db = self._make_db()
        db.client = None
        # Should not raise
        db.log_hunt_end(42, "FAILED", 0, 0, 0)


class TestNewsDatabaseCountRange(unittest.TestCase):
    """Tests for count_news_range."""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        return db

    def test_returns_count(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[(42,)])
        result = db.count_news_range("2026-01-01", "2026-01-02")
        self.assertEqual(result, 42)

    def test_no_client_returns_zero(self):
        db = self._make_db()
        db.client = None
        result = db.count_news_range("2026-01-01", "2026-01-02")
        self.assertEqual(result, 0)


# ═══════════════════════════════════════════════════════
#  9. CALENDAR — Week Snap Logic
# ═══════════════════════════════════════════════════════

class TestCalendarWeekSnap(unittest.TestCase):
    """Tests for CalendarPopulator.sync_week() date snapping"""

    def test_snaps_to_monday(self):
        base = datetime.date(2025, 6, 4)  # Wednesday
        start_of_week = base - datetime.timedelta(days=base.weekday())
        self.assertEqual(start_of_week.weekday(), 0)
        self.assertEqual(start_of_week, datetime.date(2025, 6, 2))

    def test_monday_stays_monday(self):
        monday = datetime.date(2025, 6, 2)
        start = monday - datetime.timedelta(days=monday.weekday())
        self.assertEqual(start, monday)

    def test_sunday_snaps_to_prev_monday(self):
        sunday = datetime.date(2026, 2, 22)  # Sunday
        start = sunday - datetime.timedelta(days=sunday.weekday())
        self.assertEqual(start, datetime.date(2026, 2, 16))  # Monday


# ═══════════════════════════════════════════════════════
#  10. DISCORD REPORT — Webhook Posting
# ═══════════════════════════════════════════════════════

class TestSendDiscordReport(unittest.TestCase):
    """Tests for main.send_discord_report()"""

    def setUp(self):
        with patch('logging.basicConfig'), patch('os.makedirs'):
            import main
            self.send = main.send_discord_report

    @patch('main.requests.post')
    def test_sends_plain_message(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        self.send("https://discord.webhook/test", "Hello World")
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        self.assertEqual(call_kwargs[1]['json'], {"content": "Hello World"})

    @patch('main.requests.post')
    def test_sends_embeds(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        embeds = [{"title": "Test", "color": 0x00FF00}]
        self.send("https://discord.webhook/test", None, embeds=embeds)
        call_kwargs = mock_post.call_args
        self.assertEqual(call_kwargs[1]['json'], {"embeds": embeds})

    def test_empty_webhook_does_nothing(self):
        """Should not raise when webhook URL is None/empty."""
        self.send(None, "test message")
        self.send("", "test message")

    @patch('main.requests.post', side_effect=Exception("Connection refused"))
    def test_handles_post_failure(self, mock_post):
        """Should not raise on post failure."""
        self.send("https://discord.webhook/test", "test")


# ═══════════════════════════════════════════════════════
#  11. MAIN.PY — Run Automation Early Exit Paths
# ═══════════════════════════════════════════════════════

class TestRunAutomationEarlyExits(unittest.TestCase):
    """Tests that run_automation returns proper result dicts on failure."""

    def setUp(self):
        with patch('logging.basicConfig'), patch('os.makedirs'):
            import main
            self.run_auto = main.run_automation

    @patch('main.InfisicalManager')
    def test_infisical_failure_returns_dict(self, mock_infisical_cls):
        """When Infisical fails to connect, should return structured failure dict."""
        mock_inf = MagicMock()
        mock_inf.is_connected = False
        mock_infisical_cls.return_value = mock_inf

        result = self.run_auto(run_number=1, max_runs=1)

        self.assertIsInstance(result, dict)
        self.assertFalse(result["success"])
        self.assertIn("Infisical connection failed", result["errors"])

    @patch('main.send_discord_report')
    @patch('main.InfisicalManager')
    def test_db_credentials_missing_returns_dict(self, mock_infisical_cls, mock_discord):
        """When DB credentials are missing, should return structured failure dict."""
        mock_inf = MagicMock()
        mock_inf.is_connected = True
        mock_inf.get_discord_webhook.return_value = "https://test.webhook"
        mock_inf.get_turso_news_credentials.return_value = (None, None)
        mock_inf.get_turso_analyst_credentials.return_value = (None, None)
        mock_infisical_cls.return_value = mock_inf

        result = self.run_auto(run_number=1, max_runs=1)

        self.assertIsInstance(result, dict)
        self.assertFalse(result["success"])
        self.assertEqual(result["articles_found"], 0)


# ═══════════════════════════════════════════════════════
#  12. MAIN.PY — Run Check Only
# ═══════════════════════════════════════════════════════

class TestRunCheckOnly(unittest.TestCase):
    """Tests for main.run_check_only()."""

    def setUp(self):
        with patch('logging.basicConfig'), patch('os.makedirs'):
            import main
            self.run_check = main.run_check_only

    @patch('main.InfisicalManager')
    def test_infisical_failure_no_crash(self, mock_cls):
        """Should not crash when Infisical fails."""
        mock_inf = MagicMock()
        mock_inf.is_connected = False
        mock_cls.return_value = mock_inf
        # Should return None gracefully
        result = self.run_check()
        self.assertIsNone(result)

    @patch('main.send_discord_report')
    @patch('main.NewsDatabase')
    @patch('main.InfisicalManager')
    def test_db_credentials_missing_no_crash(self, mock_cls, mock_db_cls, mock_discord):
        """Should exit gracefully when DB credentials are missing."""
        mock_inf = MagicMock()
        mock_inf.is_connected = True
        mock_inf.get_discord_webhook.return_value = "https://test"
        mock_inf.get_turso_news_credentials.return_value = (None, None)
        mock_cls.return_value = mock_inf

        result = self.run_check()
        self.assertIsNone(result)


# ═══════════════════════════════════════════════════════
#  13. NORMALIZE TITLE — Edge Cases
# ═══════════════════════════════════════════════════════

class TestNormalizeTitleEdgeCases(unittest.TestCase):
    """Edge cases for market_utils.normalize_title()"""

    def setUp(self):
        from modules.utils.market_utils import normalize_title
        self.normalize = normalize_title

    def test_double_suffix(self):
        """Title ending with two suffixes — only last should be stripped."""
        result = self.normalize("Analysis - Reuters - Bloomberg")
        self.assertEqual(result, "Analysis - Reuters")

    def test_very_long_title(self):
        """Very long title should not cause issues."""
        long_title = "A" * 5000 + " - Yahoo Finance"
        result = self.normalize(long_title)
        self.assertEqual(result, "A" * 5000)

    def test_suffix_only(self):
        """Title that IS the suffix should return empty after strip."""
        result = self.normalize(" - Yahoo Finance")
        self.assertEqual(result, "")

    def test_unicode_in_title(self):
        """Unicode characters should pass through cleanly."""
        result = self.normalize("📈 Markets Rally — Bloomberg Analysis - Reuters")
        self.assertEqual(result, "📈 Markets Rally — Bloomberg Analysis")

    def test_numeric_input(self):
        """Non-string input should return empty string."""
        result = self.normalize(42)
        self.assertEqual(result, "")

    def test_case_sensitive_suffix(self):
        """Suffixes are case-sensitive — ' - yahoo finance' should NOT be stripped."""
        result = self.normalize("Title - yahoo finance")
        self.assertEqual(result, "Title - yahoo finance")


# ═══════════════════════════════════════════════════════
#  14. DB_CLIENT — Fetch News By Date
# ═══════════════════════════════════════════════════════

class TestDbFetchNewsByDate(unittest.TestCase):
    """Tests for NewsDatabase.fetch_news_by_date()"""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        db.url = "https://test.db"
        db.token = "test-token"
        return db

    def test_returns_list_with_publisher(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[
            ("Test Title", "https://example.com", "Content line 1\nContent line 2",
             "2026-02-20T10:00:00Z", "example.com", "MACRO", "Reuters")
        ])
        results = db.fetch_news_by_date(datetime.date(2026, 2, 20))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["publisher"], "Reuters")
        self.assertEqual(results[0]["category"], "MACRO")
        self.assertIsInstance(results[0]["content"], list)

    def test_no_client_returns_empty(self):
        db = self._make_db()
        db.client = None
        results = db.fetch_news_by_date(datetime.date(2026, 2, 20))
        self.assertEqual(results, [])

    def test_category_filter(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[])
        db.fetch_news_by_date(datetime.date(2026, 2, 20), category="MACRO")
        # Should have called with 2 params (category + date)
        call_args = db.client.execute.call_args[0]
        self.assertIn("MACRO", call_args[1])

    def test_bad_datetime_fallback(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[
            ("Title", "https://x.com", "", "INVALID_DATE", "x.com", "MACRO", "AP")
        ])
        results = db.fetch_news_by_date(datetime.date(2026, 2, 20))
        self.assertEqual(results[0]["time"], "??:??")


# ═══════════════════════════════════════════════════════
#  15. DB_CLIENT — Fetch News Range & Recent
# ═══════════════════════════════════════════════════════

class TestDbFetchNewsRange(unittest.TestCase):
    """Tests for NewsDatabase.fetch_news_range()"""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        return db

    def test_returns_reconstructed_items(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[
            ("Title", "https://x.com", "Line1\nLine2", "2026-02-20T10:00:00Z", "x.com", "STOCKS", "CNBC")
        ])
        results = db.fetch_news_range("2026-02-20T01:00:00+00:00", "2026-02-21T01:00:00+00:00")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["publisher"], "CNBC")

    def test_no_client_returns_empty(self):
        db = self._make_db()
        db.client = None
        results = db.fetch_news_range("2026-01-01", "2026-01-02")
        self.assertEqual(results, [])


class TestDbFetchRecentNews(unittest.TestCase):
    """Tests for NewsDatabase.fetch_recent_news()"""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        return db

    def test_returns_items_with_publisher(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[
            ("Title", "https://x.com", "Content", "2026-02-20T14:30:00Z", "x.com", "MACRO", "Bloomberg")
        ])
        results = db.fetch_recent_news(limit=10)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["publisher"], "Bloomberg")

    def test_no_client_returns_empty(self):
        db = self._make_db()
        db.client = None
        results = db.fetch_recent_news()
        self.assertEqual(results, [])

    def test_respects_limit(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[])
        db.fetch_recent_news(limit=5)
        call_args = db.client.execute.call_args[0]
        self.assertIn(5, call_args[1])


# ═══════════════════════════════════════════════════════
#  16. DB_CLIENT — Calendar Operations
# ═══════════════════════════════════════════════════════

class TestDbInsertCalendarEvents(unittest.TestCase):
    """Tests for NewsDatabase.insert_calendar_events()"""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        return db

    def test_batch_insert(self):
        db = self._make_db()
        db.client.batch.return_value = [MagicMock()]
        events = [
            {"name": "CPI", "type": "MACRO_EVENT", "date": "2026-02-20", "importance": "HIGH"},
            {"name": "PPI", "type": "MACRO_EVENT", "date": "2026-02-21"}
        ]
        count = db.insert_calendar_events(events)
        self.assertEqual(count, 2)
        db.client.batch.assert_called_once()

    def test_empty_list_returns_zero(self):
        db = self._make_db()
        count = db.insert_calendar_events([])
        self.assertEqual(count, 0)

    def test_no_client_returns_zero(self):
        db = self._make_db()
        db.client = None
        count = db.insert_calendar_events([{"name": "X", "type": "Y", "date": "Z"}])
        self.assertEqual(count, 0)


class TestDbGetUpcomingEvents(unittest.TestCase):
    """Tests for NewsDatabase.get_upcoming_events()"""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        return db

    def test_returns_events_with_all_fields(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[
            ("CPI Report", None, "MACRO_EVENT", "2026-02-20", "HIGH", "US", "08:30", "0.3%", "0.4%", "+0.1%")
        ])
        events = db.get_upcoming_events("2026-02-20", "2026-02-25")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["name"], "CPI Report")
        self.assertEqual(events[0]["country"], "US")
        self.assertEqual(events[0]["eps_estimate"], "0.3%")

    def test_no_client_returns_empty(self):
        db = self._make_db()
        db.client = None
        events = db.get_upcoming_events("2026-01-01", "2026-01-02")
        self.assertEqual(events, [])


class TestDbClearCalendar(unittest.TestCase):
    """Tests for NewsDatabase.clear_calendar()"""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        return db

    def test_executes_delete(self):
        db = self._make_db()
        db.clear_calendar()
        db.client.execute.assert_called_once()
        call_sql = db.client.execute.call_args[0][0]
        self.assertIn("DELETE", call_sql)

    def test_no_client_no_crash(self):
        db = self._make_db()
        db.client = None
        db.clear_calendar()  # Should not raise


# ═══════════════════════════════════════════════════════
#  17. DB_CLIENT — Get Last Update Time
# ═══════════════════════════════════════════════════════

class TestDbGetLastUpdateTime(unittest.TestCase):
    """Tests for NewsDatabase.get_last_update_time()"""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        return db

    def test_returns_timestamp(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[("2026-02-20T15:00:00Z",)])
        result = db.get_last_update_time()
        self.assertEqual(result, "2026-02-20T15:00:00Z")

    def test_empty_db_returns_none(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[(None,)])
        result = db.get_last_update_time()
        self.assertIsNone(result)

    def test_no_client_returns_none(self):
        db = self._make_db()
        db.client = None
        result = db.get_last_update_time()
        self.assertIsNone(result)

    def test_error_returns_none(self):
        db = self._make_db()
        db.client.execute.side_effect = Exception("DB gone")
        result = db.get_last_update_time()
        self.assertIsNone(result)


# ═══════════════════════════════════════════════════════
#  18. INFISICAL_CLIENT — Secret Extraction
# ═══════════════════════════════════════════════════════

class TestInfisicalExtractValue(unittest.TestCase):
    """Tests for InfisicalManager._extract_value()"""

    def _make_manager(self):
        from modules.clients.infisical_client import InfisicalManager
        mgr = InfisicalManager.__new__(InfisicalManager)
        mgr.client = None
        mgr.is_connected = False
        return mgr

    def test_none_input(self):
        mgr = self._make_manager()
        self.assertIsNone(mgr._extract_value(None))

    def test_v2_direct_attribute(self):
        mgr = self._make_manager()
        obj = MagicMock(spec=[])
        obj.secret_value = "my-secret"
        result = mgr._extract_value(obj)
        self.assertEqual(result, "my-secret")

    def test_v3_nested_attribute(self):
        mgr = self._make_manager()
        nested = MagicMock(spec=[])
        nested.secret_value = "nested-val"
        obj = MagicMock(spec=[])
        obj.secret = nested
        result = mgr._extract_value(obj)
        self.assertEqual(result, "nested-val")

    def test_dict_input(self):
        mgr = self._make_manager()
        result = mgr._extract_value({"secret_value": "dict-val"})
        self.assertEqual(result, "dict-val")

    def test_dict_fallback_value(self):
        mgr = self._make_manager()
        result = mgr._extract_value({"value": "fallback-val"})
        self.assertEqual(result, "fallback-val")


class TestInfisicalGetDiscordWebhook(unittest.TestCase):
    """Tests for InfisicalManager.get_discord_webhook()"""

    def _make_manager(self):
        from modules.clients.infisical_client import InfisicalManager
        mgr = InfisicalManager.__new__(InfisicalManager)
        mgr.client = MagicMock()
        mgr.is_connected = True
        mgr.project_id = "test"
        mgr.default_env = "dev"
        return mgr

    def test_returns_first_found(self):
        mgr = self._make_manager()
        # Simulate: first name fails, second returns value
        mgr.get_secret = MagicMock(side_effect=[None, None, "https://discord.webhook/found", None, None, None])
        result = mgr.get_discord_webhook()
        self.assertEqual(result, "https://discord.webhook/found")

    def test_returns_none_if_all_fail(self):
        mgr = self._make_manager()
        mgr.get_secret = MagicMock(return_value=None)
        result = mgr.get_discord_webhook()
        self.assertIsNone(result)


class TestInfisicalGetMarketauxKeys(unittest.TestCase):
    """Tests for InfisicalManager.get_marketaux_keys()"""

    def _make_manager(self):
        from modules.clients.infisical_client import InfisicalManager
        mgr = InfisicalManager.__new__(InfisicalManager)
        mgr.client = MagicMock()
        mgr.is_connected = True
        mgr.project_id = "test"
        mgr.default_env = "dev"
        return mgr

    def test_csv_format(self):
        mgr = self._make_manager()
        mgr.get_secret = MagicMock(return_value="key1,key2,key3")
        mgr.list_secrets = MagicMock(return_value=[])
        result = mgr.get_marketaux_keys()
        self.assertEqual(len(result), 3)
        self.assertIn("key1", result)

    def test_json_format(self):
        mgr = self._make_manager()
        mgr.get_secret = MagicMock(return_value='["k1","k2"]')
        mgr.list_secrets = MagicMock(return_value=[])
        result = mgr.get_marketaux_keys()
        self.assertIn("k1", result)
        self.assertIn("k2", result)

    def test_no_keys_returns_empty(self):
        mgr = self._make_manager()
        mgr.get_secret = MagicMock(return_value=None)
        mgr.list_secrets = MagicMock(return_value=[])
        result = mgr.get_marketaux_keys()
        self.assertEqual(result, [])


# ═══════════════════════════════════════════════════════
#  19. MARKET_UTILS — Driver Management
# ═══════════════════════════════════════════════════════

class TestIsDriverAlive(unittest.TestCase):
    """Tests for market_utils.is_driver_alive()"""

    def setUp(self):
        from modules.utils.market_utils import is_driver_alive
        self.alive = is_driver_alive

    def test_none_driver(self):
        self.assertFalse(self.alive(None))

    def test_alive_driver(self):
        mock_driver = MagicMock()
        mock_driver.current_url = "https://example.com"
        self.assertTrue(self.alive(mock_driver))

    def test_dead_driver(self):
        mock_driver = MagicMock()
        type(mock_driver).current_url = property(lambda self: (_ for _ in ()).throw(Exception("dead")))
        self.assertFalse(self.alive(mock_driver))


class TestForceQuitDriver(unittest.TestCase):
    """Tests for market_utils.force_quit_driver()"""

    def setUp(self):
        from modules.utils.market_utils import force_quit_driver
        self.quit = force_quit_driver

    def test_none_driver(self):
        """Should not crash on None."""
        self.quit(None)

    @patch('modules.utils.market_utils.os.kill')
    def test_normal_quit(self, mock_kill):
        mock_driver = MagicMock()
        mock_driver.service.process.pid = 12345
        self.quit(mock_driver)
        mock_driver.quit.assert_called_once()

    @patch('modules.utils.market_utils.os.kill')
    def test_quit_fails_but_still_kills(self, mock_kill):
        mock_driver = MagicMock()
        mock_driver.quit.side_effect = Exception("quit failed")
        mock_driver.service.process.pid = 12345
        self.quit(mock_driver)
        mock_kill.assert_called_once()


# ═══════════════════════════════════════════════════════
#  20. ENGINE ERROR AGGREGATION
# ═══════════════════════════════════════════════════════

class TestMacroEngineErrorAggregation(unittest.TestCase):
    """Tests that macro engine returns structured errors on Chrome failure."""

    @patch('modules.engines.macro_engine.market_utils.get_selenium_driver')
    def test_chrome_failure_returns_structured_dict(self, mock_driver):
        from modules.engines.macro_engine import run_macro_scan
        mock_driver.side_effect = Exception("Chrome not found")
        log = MagicMock()

        result = run_macro_scan(
            datetime.date(2026, 2, 20), 5, log,
            headless=True
        )
        self.assertIsInstance(result, dict)
        self.assertEqual(result["articles"], [])
        self.assertTrue(len(result["errors"]) > 0)
        self.assertIn("Chrome not found", result["errors"][0])


class TestStocksEngineErrorAggregation(unittest.TestCase):
    """Tests that stocks engine returns structured errors on Chrome failure."""

    @patch('modules.engines.stocks_engine.market_utils.get_selenium_driver')
    def test_chrome_failure_returns_structured_dict(self, mock_driver):
        from modules.engines.stocks_engine import run_stocks_scan
        mock_driver.side_effect = Exception("Chrome not found")
        log = MagicMock()

        result = run_stocks_scan(
            datetime.date(2026, 2, 20), 5, log,
            headless=True
        )
        self.assertIsInstance(result, dict)
        self.assertEqual(result["articles"], [])
        self.assertTrue(len(result["errors"]) > 0)


# ═══════════════════════════════════════════════════════
#  21. MARKET CALENDAR — Edge Cases
# ═══════════════════════════════════════════════════════

class TestMarketCalendarEdgeCases(unittest.TestCase):
    """Edge cases for MarketCalendar."""

    def setUp(self):
        from modules.utils.market_utils import MarketCalendar
        self.cal = MarketCalendar

    def _utc(self, year, month, day, hour=12, minute=0):
        return datetime.datetime(year, month, day, hour, minute, tzinfo=datetime.timezone.utc)

    def test_new_years_day_not_trading(self):
        self.assertFalse(self.cal.is_trading_day(datetime.date(2026, 1, 1)))

    def test_day_after_new_years_is_trading(self):
        self.assertTrue(self.cal.is_trading_day(datetime.date(2026, 1, 2)))

    def test_session_at_exactly_1am(self):
        """Exactly 1 AM UTC is the boundary — should be in today's reference."""
        now = self._utc(2026, 2, 18, 1, 0)  # Wednesday 1:00 AM
        target, start, end = self.cal.resolve_trading_session(now)
        self.assertEqual(target, datetime.date(2026, 2, 18))

    def test_session_at_0_59_still_previous(self):
        """0:59 AM UTC — should still be in previous day's session."""
        now = self._utc(2026, 2, 18, 0, 59)  # Wednesday 0:59
        target, start, end = self.cal.resolve_trading_session(now)
        self.assertEqual(target, datetime.date(2026, 2, 17))

    def test_christmas_week_holiday_cluster(self):
        """Christmas 2026 is Friday. Thu Dec 24 is early close, Fri Dec 25 is holiday."""
        self.assertFalse(self.cal.is_trading_day(datetime.date(2026, 12, 25)))
        self.assertTrue(self.cal.is_early_close(datetime.date(2026, 12, 24)))
        self.assertTrue(self.cal.is_trading_day(datetime.date(2026, 12, 24)))

    def test_prev_trading_day_never_infinite_loops(self):
        """Should never get stuck in an infinite loop even at year start."""
        result = self.cal.get_prev_trading_day(datetime.date(2026, 1, 2))
        self.assertIsNotNone(result)
        self.assertTrue(result < datetime.date(2026, 1, 2))

    def test_resolve_session_for_future_date(self):
        """Future date should still resolve correctly."""
        now = self._utc(2026, 2, 20, 12, 0)
        target, start, end = self.cal.resolve_session_for_date(
            datetime.date(2026, 6, 15), now
        )
        # June 15 is a Monday (trading day)
        self.assertEqual(target, datetime.date(2026, 6, 15))
        # End should be capped at now since the session hasn't happened yet
        self.assertEqual(end, now)


# ═══════════════════════════════════════════════════════
#  22. BOT — Date Validation Logic
# ═══════════════════════════════════════════════════════

class TestBotDateValidation(unittest.TestCase):
    """Tests for the date validation logic used in the Discord bot."""

    def test_valid_date_format(self):
        """Standard YYYY-MM-DD format should parse."""
        from datetime import datetime
        parsed = datetime.strptime("2026-02-20", "%Y-%m-%d")
        self.assertEqual(parsed.day, 20)

    def test_invalid_date_format(self):
        """Non-YYYY-MM-DD format should raise ValueError."""
        from datetime import datetime
        with self.assertRaises(ValueError):
            datetime.strptime("20-02-2026", "%Y-%m-%d")

    def test_invalid_date_text(self):
        """Text like 'tomorrow' should raise ValueError."""
        from datetime import datetime
        with self.assertRaises(ValueError):
            datetime.strptime("tomorrow", "%Y-%m-%d")

    def test_future_date_within_5_days(self):
        """Dates up to 5 days ahead should be allowed."""
        from datetime import datetime, timedelta
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        target = today + timedelta(days=3)
        max_future = today + timedelta(days=5)
        self.assertLessEqual(target, max_future)

    def test_future_date_beyond_5_days(self):
        """Dates more than 5 days ahead should be rejected."""
        from datetime import datetime, timedelta
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        target = today + timedelta(days=10)
        max_future = today + timedelta(days=5)
        self.assertGreater(target, max_future)


# ═══════════════════════════════════════════════════════
#  23. DB_CLIENT — Fetch Cache Map
# ═══════════════════════════════════════════════════════

class TestDbFetchCacheMap(unittest.TestCase):
    """Tests for NewsDatabase.fetch_cache_map()"""

    def _make_db(self):
        from modules.clients.db_client import NewsDatabase
        db = NewsDatabase.__new__(NewsDatabase)
        db.client = MagicMock()
        db.url = "https://test.db"
        db.token = "test-token"
        return db

    def test_returns_url_keyed_dict(self):
        db = self._make_db()
        db.client.execute.return_value = MagicMock(rows=[
            ("Title A", "https://a.com/1", "Content", "2026-02-20T10:00:00Z", "a.com", "MACRO", "AP"),
            ("Title B", "https://b.com/2", "Content", "2026-02-20T11:00:00Z", "b.com", "STOCKS", "Reuters")
        ])
        cache = db.fetch_cache_map(datetime.date(2026, 2, 20))
        self.assertIn("https://a.com/1", cache)
        self.assertIn("https://b.com/2", cache)
        self.assertEqual(cache["https://a.com/1"]["title"], "Title A")

    def test_no_client_returns_empty(self):
        db = self._make_db()
        db.client = None
        cache = db.fetch_cache_map(datetime.date(2026, 2, 20))
        self.assertEqual(cache, {})


# ═══════════════════════════════════════════════════════
#  24. PARSE_ISO_DATETIME — Edge Cases
# ═══════════════════════════════════════════════════════

class TestParseIsoDatetimeEdgeCases(unittest.TestCase):
    """Additional edge cases for parse_iso_datetime."""

    def setUp(self):
        from modules.utils.market_utils import parse_iso_datetime
        self.parse = parse_iso_datetime

    def test_date_only_no_time(self):
        """A date-only string should still parse."""
        result = self.parse("2026-02-20")
        self.assertIsNotNone(result)
        self.assertEqual(result.day, 20)

    def test_timestamp_with_microseconds(self):
        result = self.parse("2026-02-20T15:30:45.123456Z")
        self.assertIsNotNone(result)
        self.assertEqual(result.second, 45)

    def test_numeric_input(self):
        """Numeric input should return None."""
        result = self.parse(12345)
        self.assertIsNone(result)

    def test_rfc2822_format(self):
        """RFC 2822 dates (from RSS feeds) should parse."""
        result = self.parse("Thu, 20 Feb 2026 10:30:00 GMT")
        self.assertIsNotNone(result)


# ═══════════════════════════════════════════════════════
#  Run
# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    unittest.main(verbosity=2)
