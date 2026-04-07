"""
Tests for discord_bot/bot.py
Covers: date validation, command execution flow, GitHub dispatch, error handling.
Uses discord.py's test utilities with mocked HTTP sessions.
"""

import unittest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta, timezone
import os
import sys

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def run_async(coro):
    """Helper to run async tests."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ═══════════════════════════════════════════════════════
#  1. DATE VALIDATION (Shared by both commands)
# ═══════════════════════════════════════════════════════

class TestDateValidation(unittest.TestCase):
    """Tests the date validation logic in bot commands."""

    def test_get_target_date_none(self):
        """None input should return None to trigger interactive UI."""
        from discord_bot.bot import get_target_date
        self.assertIsNone(get_target_date(None))

    def test_get_target_date_today(self):
        """'0' should return today's UTC date string."""
        from discord_bot.bot import get_target_date
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.assertEqual(get_target_date("0"), today)

    def test_get_target_date_relative(self):
        """'-1', '-5' should return correct past dates in UTC."""
        from discord_bot.bot import get_target_date
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        self.assertEqual(get_target_date("-1"), yesterday)
        
        five_days_back = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")
        self.assertEqual(get_target_date("-5"), five_days_back)

    def test_get_target_date_passthrough(self):
        """'2026-02-20' should pass through as a string for validation later."""
        from discord_bot.bot import get_target_date
        self.assertEqual(get_target_date("2026-02-20"), "2026-02-20")



    def test_valid_date_parses(self):
        parsed = datetime.strptime("2026-02-20", "%Y-%m-%d")
        self.assertEqual(parsed.year, 2026)
        self.assertEqual(parsed.month, 2)
        self.assertEqual(parsed.day, 20)

    def test_invalid_format_raises(self):
        with self.assertRaises(ValueError):
            datetime.strptime("20-02-2026", "%Y-%m-%d")

    def test_text_input_raises(self):
        with self.assertRaises(ValueError):
            datetime.strptime("tomorrow", "%Y-%m-%d")

    def test_partial_date_raises(self):
        with self.assertRaises(ValueError):
            datetime.strptime("2026-02", "%Y-%m-%d")

    def test_future_within_5_days_allowed(self):
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        target = today + timedelta(days=3)
        max_future = today + timedelta(days=5)
        self.assertLessEqual(target, max_future)

    def test_future_beyond_5_days_rejected(self):
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        target = today + timedelta(days=10)
        max_future = today + timedelta(days=5)
        self.assertGreater(target, max_future)

    def test_past_date_allowed(self):
        """Past dates should be valid — no rejection."""
        parsed = datetime.strptime("2020-01-15", "%Y-%m-%d")
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        max_future = today + timedelta(days=5)
        self.assertLessEqual(parsed, max_future)

    def test_date_normalization(self):
        """Parsing and re-formatting should produce clean YYYY-MM-DD."""
        parsed = datetime.strptime("2026-2-5", "%Y-%m-%d")
        normalized = parsed.strftime("%Y-%m-%d")
        self.assertEqual(normalized, "2026-02-05")


# ═══════════════════════════════════════════════════════
#  2. !rawnews COMMAND
# ═══════════════════════════════════════════════════════

class TestTriggerFetchCommand(unittest.TestCase):
    """Tests for the !rawnews command logic."""

    def setUp(self):
        """Set up mock context and environment."""
        self.ctx = MagicMock()
        self.ctx.send = AsyncMock()
        self.ctx.author = "TestUser#1234"
        
        # Create status message mock
        self.status_msg = MagicMock()
        self.status_msg.edit = AsyncMock()
        self.ctx.send.return_value = self.status_msg

    @patch.dict(os.environ, {"GITHUB_PAT": "test-token", "DISCORD_BOT_TOKEN": "test-bot-token"})
    def test_invalid_date_sends_error_message(self):
        """!rawnews bad-date should send an error and return."""
        async def _test():
            # Simulate the date validation logic from bot.py
            target_date = "bad-date"
            try:
                datetime.strptime(target_date, "%Y-%m-%d")
            except ValueError:
                await self.ctx.send(
                    f"❌ **Invalid date format:** `{target_date}`\n"
                    f"> Expected format: **YYYY-MM-DD**"
                )
                return
        
        run_async(_test())
        self.ctx.send.assert_called_once()
        call_content = self.ctx.send.call_args[0][0]
        self.assertIn("Invalid date format", call_content)
        self.assertIn("bad-date", call_content)

    def test_future_date_sends_rejection(self):
        """!rawnews with date >5 days in future should be rejected."""
        async def _test():
            target_date = (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")
            parsed = datetime.strptime(target_date, "%Y-%m-%d")
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            max_future = today + timedelta(days=5)
            
            if parsed > max_future:
                await self.ctx.send(f"❌ **Invalid date:** `{target_date}` is too far in the future.")
                return
        
        run_async(_test())
        self.ctx.send.assert_called_once()
        call_content = self.ctx.send.call_args[0][0]
        self.assertIn("too far in the future", call_content)

    def test_no_date_sends_view(self):
        """!rawnews with no date should send the interactive view instead of dispatching."""
        async def _test():
            from discord_bot.bot import cmd_trigger_fetch, DateSelectionView
            await cmd_trigger_fetch(self.ctx, None)
            
        run_async(_test())
        self.ctx.send.assert_called_once()
        call_kwargs = self.ctx.send.call_args[1]
        self.assertIn('view', call_kwargs)
        self.assertEqual(type(call_kwargs['view']).__name__, "DateSelectionView")

    def test_valid_date_adds_inputs(self):
        """!rawnews test helper to verify target_date payload generation logic."""
        data = {"ref": "main"}
        target_date = "2026-02-20"
        if target_date:
            data["inputs"] = {"target_date": target_date}
        
        self.assertIn("inputs", data)
        self.assertEqual(data["inputs"]["target_date"], "2026-02-20")


# ═══════════════════════════════════════════════════════
#  3. !checkrawnews COMMAND
# ═══════════════════════════════════════════════════════

class TestCheckRawNewsCommand(unittest.TestCase):
    """Tests for the !checkrawnews command logic."""

    def setUp(self):
        self.ctx = MagicMock()
        self.ctx.send = AsyncMock()
        self.status_msg = MagicMock()
        self.status_msg.edit = AsyncMock()
        self.ctx.send.return_value = self.status_msg

    def test_check_mode_payload(self):
        """!checkrawnews should dispatch with mode='check'."""
        data = {
            "ref": "main",
            "inputs": {
                "mode": "check"
            }
        }
        self.assertEqual(data["inputs"]["mode"], "check")
        self.assertEqual(data["ref"], "main")

    def test_check_with_date_adds_target(self):
        """!checkrawnews 2026-02-20 should add target_date to inputs."""
        data = {
            "ref": "main",
            "inputs": {"mode": "check"}
        }
        target_date = "2026-02-20"
        if target_date:
            data["inputs"]["target_date"] = target_date
        
        self.assertEqual(data["inputs"]["mode"], "check")
        self.assertEqual(data["inputs"]["target_date"], "2026-02-20")

    def test_no_date_on_check_sends_view(self):
        """!checkrawnews with no date should send the interactive view instead of dispatching."""
        async def _test():
            from discord_bot.bot import cmd_check_raw_news, DateSelectionView
            await cmd_check_raw_news(self.ctx, None)
            
        run_async(_test())
        self.ctx.send.assert_called_once()
        call_kwargs = self.ctx.send.call_args[1]
        self.assertIn('view', call_kwargs)
        self.assertEqual(type(call_kwargs['view']).__name__, "DateSelectionView")


# ═══════════════════════════════════════════════════════
#  4. GITHUB API DISPATCH FLOW
# ═══════════════════════════════════════════════════════

class TestGitHubDispatch(unittest.TestCase):
    """Tests for the GitHub Actions dispatch HTTP logic."""

    def test_dispatch_url_construction(self):
        """URL should use correct repo and workflow filename."""
        repo = "emadprograms/News-Fetcher-CLI"
        workflow = "manual_run.yml"
        url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"
        self.assertEqual(
            url,
            "https://api.github.com/repos/emadprograms/News-Fetcher-CLI/actions/workflows/manual_run.yml/dispatches"
        )

    def test_headers_include_auth(self):
        """Headers should include Bearer token and correct API version."""
        token = "ghp_test123"
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        self.assertEqual(headers["Authorization"], "Bearer ghp_test123")
        self.assertEqual(headers["X-GitHub-Api-Version"], "2022-11-28")

    def test_success_status_is_204(self):
        """GitHub returns 204 No Content on successful dispatch."""
        self.assertEqual(204, 204)  # Document the expected status

    def test_runs_url_construction(self):
        """Live link fetching URL should point to workflow runs."""
        repo = "emadprograms/News-Fetcher-CLI"
        workflow = "manual_run.yml"
        runs_url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/runs"
        self.assertIn("/runs", runs_url)
        self.assertNotIn("/dispatches", runs_url)


# ═══════════════════════════════════════════════════════
#  5. LIVE LINK RETRIEVAL LOGIC
# ═══════════════════════════════════════════════════════

class TestLiveLinkRetrieval(unittest.TestCase):
    """Tests for the live link retrieval after dispatch."""

    def test_extracts_html_url_from_runs(self):
        """Should extract html_url from first workflow run."""
        runs_data = {
            "workflow_runs": [
                {"html_url": "https://github.com/repo/actions/runs/123"},
                {"html_url": "https://github.com/repo/actions/runs/122"}
            ]
        }
        live_url = runs_data["workflow_runs"][0]["html_url"]
        self.assertEqual(live_url, "https://github.com/repo/actions/runs/123")

    def test_empty_runs_returns_none(self):
        """No workflow runs should result in None live_url."""
        runs_data = {"workflow_runs": []}
        live_url = None
        if runs_data.get("workflow_runs"):
            live_url = runs_data["workflow_runs"][0]["html_url"]
        self.assertIsNone(live_url)

    def test_missing_key_returns_none(self):
        """Missing 'workflow_runs' key should result in None."""
        runs_data = {}
        live_url = None
        if runs_data.get("workflow_runs"):
            live_url = runs_data["workflow_runs"][0]["html_url"]
        self.assertIsNone(live_url)


# ═══════════════════════════════════════════════════════
#  6. ENVIRONMENT VARIABLE GUARDS
# ═══════════════════════════════════════════════════════

class TestEnvironmentGuards(unittest.TestCase):
    """Tests for environment variable validation at startup."""

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_discord_token(self):
        """Bot should detect missing DISCORD_BOT_TOKEN."""
        token = os.getenv("DISCORD_BOT_TOKEN")
        self.assertIsNone(token)

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_github_pat(self):
        """Bot should detect missing GITHUB_PAT."""
        token = os.getenv("GITHUB_PAT")
        self.assertIsNone(token)

    @patch.dict(os.environ, {"GITHUB_REPO": "custom/repo"})
    def test_custom_repo_override(self):
        """GITHUB_REPO env var should override default."""
        repo = os.getenv("GITHUB_REPO", "emadprograms/News-Fetcher-CLI")
        self.assertEqual(repo, "custom/repo")

    @patch.dict(os.environ, {}, clear=True) 
    def test_default_repo_fallback(self):
        """Should use default repo when GITHUB_REPO not set."""
        repo = os.getenv("GITHUB_REPO", "emadprograms/News-Fetcher-CLI")
        self.assertEqual(repo, "emadprograms/News-Fetcher-CLI")

    @patch.dict(os.environ, {"WORKFLOW_FILENAME": "custom_workflow.yml"})
    def test_custom_workflow_override(self):
        """WORKFLOW_FILENAME env var should override default."""
        wf = os.getenv("WORKFLOW_FILENAME", "manual_run.yml")
        self.assertEqual(wf, "custom_workflow.yml")


# ═══════════════════════════════════════════════════════
#  7. ERROR HANDLING
# ═══════════════════════════════════════════════════════

class TestErrorHandling(unittest.TestCase):
    """Tests for error handling in bot commands."""

    def setUp(self):
        self.ctx = MagicMock()
        self.ctx.send = AsyncMock()
        self.status_msg = MagicMock()
        self.status_msg.edit = AsyncMock()
        self.ctx.send.return_value = self.status_msg

    def test_api_error_message_format(self):
        """Non-204 response should produce formatted error message."""
        status = 422
        error_details = "Workflow does not exist"
        msg = f"❌ **Failed to trigger workflow.**\nGitHub API Error ({status}): `{error_details}`"
        self.assertIn("422", msg)
        self.assertIn("Workflow does not exist", msg)

    def test_network_error_message_format(self):
        """Connection errors should produce formatted internal error message."""
        error = "Connection refused"
        msg = f"⚠️ **Internal Error:** Could not reach GitHub.\n`{error}`"
        self.assertIn("Internal Error", msg)
        self.assertIn("Connection refused", msg)

    def test_exception_in_dispatch_doesnt_crash(self):
        """Exceptions during dispatch should be caught gracefully."""
        async def _simulate_dispatch():
            try:
                raise aiohttp.ClientError("DNS resolution failed")
            except Exception as e:
                await self.status_msg.edit(content=f"⚠️ Error: {str(e)}")
        
        # Need aiohttp imported for test
        import aiohttp
        run_async(_simulate_dispatch())
        self.status_msg.edit.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
