import os
import discord
from discord.ext import commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Load local environment variables if present
load_dotenv()

# Configuration from Environment Variables
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_PAT")
GITHUB_REPO = os.getenv("GITHUB_REPO", "emadprograms/news-fetcher")
# Workflows
FETCH_WORKFLOW = "newsfetcher.yml"
CHECK_WORKFLOW = "news_checker.yml"

# Setup intents for message reading
intents = discord.Intents.default()
intents.message_content = True

# Initialize Bot
bot = commands.Bot(command_prefix="!", intents=intents)

# -----------------------------------------------------------------------------
# Internal Logic Helpers & UI Components
# -----------------------------------------------------------------------------

def get_target_date(date_input: str = None) -> str | None:
    """
    Parses date input. Supports:
    - None -> Returns None (Forces interactive picker)
    - "0" -> Today (UTC)
    - "-1", "-2", etc. -> Days relative to today
    - "YYYY-MM-DD" -> Specific date
    """
    if not date_input:
        return None
    
    today = datetime.now(timezone.utc)
    
    if date_input == "0":
        return today.strftime("%Y-%m-%d")
    
    # Handle relative dates (e.g. -1, -5)
    if date_input.startswith("-") and date_input[1:].isdigit():
        try:
            days_back = int(date_input[1:])
            target = today - timedelta(days=days_back)
            return target.strftime("%Y-%m-%d")
        except: pass

    return date_input # Return as-is for validation later

class CustomDateModal(discord.ui.Modal, title='Enter Custom Date'):
    def __init__(self, action_callback, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.action_callback = action_callback

    date_val = discord.ui.TextInput(
        label='Date (YYYY-MM-DD)',
        placeholder='2026-02-22',
        required=True,
        min_length=10,
        max_length=10
    )

    async def on_submit(self, interaction: discord.Interaction):
        await self.action_callback(interaction, self.date_val.value)

class DateDropdown(discord.ui.Select):
    def __init__(self, options, action_callback):
        super().__init__(placeholder="📅 Select a date...", min_values=1, max_values=1, options=options)
        self.action_callback = action_callback

    async def callback(self, interaction: discord.Interaction):
        # Defuse the view when an option is selected
        await interaction.response.edit_message(content=f"🗓️ **Selected Date:** {self.values[0]}\nInitializing... 🚀", view=None)
        await self.action_callback(interaction, self.values[0])

class DateSelectionView(discord.ui.View):
    def __init__(self, action_callback):
        super().__init__(timeout=180)
        self.action_callback = action_callback
        
        options = []
        today = datetime.now(timezone.utc)
        for i in range(14):
            target = today - timedelta(days=i)
            date_str = target.strftime("%Y-%m-%d")
            
            if i == 0:
                label = "Today (0)"
            elif i == 1:
                label = "Yesterday (-1)"
            else:
                day_name = target.strftime("%A")
                label = f"{day_name} (-{i})"
            
            options.append(discord.SelectOption(label=label, description=date_str, value=date_str))
        
        self.add_item(DateDropdown(options, action_callback))

    @discord.ui.button(label="⌨️ Manual Date Entry", style=discord.ButtonStyle.secondary)
    async def manual_date(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(CustomDateModal(self.action_callback))


# -----------------------------------------------------------------------------
# Bot Commands
# -----------------------------------------------------------------------------

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name} ({bot.user.id})')
    print('Bot is ready to receive commands.')

@bot.command(name="checkrawnews")
async def cmd_check_raw_news(ctx, date_indicator: str = None):
    """Triggers a session status check. Use: !checkrawnews [0 | -1 | YYYY-MM-DD]"""
    target_date = get_target_date(date_indicator)
    
    if not target_date:
        view = DateSelectionView(action_callback=_handle_check_raw_news)
        await ctx.send("🗓️ **Select Date for Status Check:**", view=view)
    else:
        await _handle_check_raw_news(ctx, target_date)

async def _handle_check_raw_news(interaction_or_ctx, target_date: str):
    # 🛡️ Validate date format BEFORE dispatching
    try:
        parsed = datetime.strptime(target_date, "%Y-%m-%d")
        
        # Allow targeting upcoming trading days (up to 5 days ahead) for weekends/holidays
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        max_future = today + timedelta(days=5)
        
        if parsed > max_future:
            msg = (f"❌ **Invalid date:** `{target_date}` is too far in the future.\n"
                   f"> You can target dates up to 5 days ahead to prepare for the next trading session.")
            if isinstance(interaction_or_ctx, discord.Interaction):
                await interaction_or_ctx.followup.send(msg, ephemeral=True) if interaction_or_ctx.response.is_done() else await interaction_or_ctx.response.send_message(msg, ephemeral=True)
            else:
                await interaction_or_ctx.send(msg)
            return
            
        target_date = parsed.strftime("%Y-%m-%d")  # Normalize to clean format
    except ValueError:
        msg = (f"❌ **Invalid date format:** `{target_date}`\n"
               f"> Expected format: **YYYY-MM-DD** (e.g. `2026-02-18`)\n"
               f"> Please try again with a valid date.")
        if isinstance(interaction_or_ctx, discord.Interaction):
            await interaction_or_ctx.followup.send(msg, ephemeral=True) if interaction_or_ctx.response.is_done() else await interaction_or_ctx.response.send_message(msg, ephemeral=True)
        else:
            await interaction_or_ctx.send(msg)
        return

    init_msg = f"📡 **Connecting to News Grid...** Dispatching status check signal for `{target_date}`."
    if isinstance(interaction_or_ctx, discord.Interaction):
        if not interaction_or_ctx.response.is_done():
            await interaction_or_ctx.response.send_message(init_msg)
        status_msg = await interaction_or_ctx.original_response()
    else:
        status_msg = await interaction_or_ctx.send(init_msg)
    
    # Prepare GitHub API request
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{CHECK_WORKFLOW}/dispatches"
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    # Trigger the workflow
    data = {
        "ref": "main",
        "inputs": {}
    }

    # Add optional target_date input if provided
    if target_date:
        data["inputs"]["target_date"] = target_date
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                if response.status == 204:
                    await status_msg.edit(content="💠 **Check Dispatched!**\n> GitHub is now querying the session status... Fetching live link... 📡")
                    
                    # Try up to 3 times with 4s wait each to get the live link
                    live_url = None
                    for attempt in range(1, 4):
                        await asyncio.sleep(4)
                        runs_url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{CHECK_WORKFLOW}/runs"
                        async with session.get(runs_url, headers=headers) as runs_resp:
                            if runs_resp.status == 200:
                                runs_data = await runs_resp.json()
                                if runs_data.get("workflow_runs"):
                                    live_url = runs_data["workflow_runs"][0]["html_url"]
                                    break
                    
                    if live_url:
                        await status_msg.edit(content=f"💠 **Check Dispatched!**\n> GitHub is now querying the session status.\n> 🔗 **[Watch Live Status Check on GitHub](<{live_url}>)**\n\n> The report will be delivered via webhook shortly. 📡")
                    else:
                        await status_msg.edit(content="💠 **Check Dispatched!**\n> GitHub is now querying the session status. (Live link could not be retrieved - check GitHub Actions manually)\n\n> The report will be delivered via webhook shortly. 📡")
                else:
                    try:
                        response_json = await response.json()
                        error_details = response_json.get("message", "No error message provided")
                    except:
                        error_details = await response.text()
                    
                    await status_msg.edit(content=f"❌ **Failed to trigger check.**\nGitHub API Error ({response.status}): `{error_details}`\n> **Workflow:** `{CHECK_WORKFLOW}`\n> **Repo:** `{GITHUB_REPO}`")
                    print(f"Failed to trigger: {response.status} - {error_details}")
    except Exception as e:
        await status_msg.edit(content=f"⚠️ **Internal Error:** Could not reach GitHub.\n`{str(e)}`")

@bot.command(name="rawnews")
async def cmd_trigger_fetch(ctx, date_indicator: str = None):
    """Triggers the news fetch. Use: !rawnews [0 | -1 | YYYY-MM-DD]"""
    target_date = get_target_date(date_indicator)
    
    if not target_date:
        view = DateSelectionView(action_callback=_handle_trigger_fetch)
        await ctx.send("🗓️ **Select Date to Fetch News For:**", view=view)
    else:
        await _handle_trigger_fetch(ctx, target_date)

async def _handle_trigger_fetch(interaction_or_ctx, target_date: str):
    # 🛡️ Validate date format BEFORE dispatching
    try:
        parsed = datetime.strptime(target_date, "%Y-%m-%d")
        
        # Allow targeting upcoming trading days (up to 5 days ahead) for weekends/holidays
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        max_future = today + timedelta(days=5)
        
        if parsed > max_future:
            msg = (f"❌ **Invalid date:** `{target_date}` is too far in the future.\n"
                   f"> You can target dates up to 5 days ahead to prepare for the next trading session.")
            if isinstance(interaction_or_ctx, discord.Interaction):
                await interaction_or_ctx.followup.send(msg, ephemeral=True) if interaction_or_ctx.response.is_done() else await interaction_or_ctx.response.send_message(msg, ephemeral=True)
            else:
                await interaction_or_ctx.send(msg)
            return
            
        target_date = parsed.strftime("%Y-%m-%d")  # Normalize to clean format
    except ValueError:
        msg = (f"❌ **Invalid date format:** `{target_date}`\n"
               f"> Expected format: **YYYY-MM-DD** (e.g. `2026-02-18`)\n"
               f"> Please try again with a valid date.")
        if isinstance(interaction_or_ctx, discord.Interaction):
            await interaction_or_ctx.followup.send(msg, ephemeral=True) if interaction_or_ctx.response.is_done() else await interaction_or_ctx.response.send_message(msg, ephemeral=True)
        else:
            await interaction_or_ctx.send(msg)
        return

    init_msg = f"📡 **Connecting to News Grid...** Dispatching signal for date: `{target_date}`"
    if isinstance(interaction_or_ctx, discord.Interaction):
        if not interaction_or_ctx.response.is_done():
            await interaction_or_ctx.response.send_message(init_msg)
        status_msg = await interaction_or_ctx.original_response()
    else:
        status_msg = await interaction_or_ctx.send(init_msg)
    
    # Prepare GitHub API request
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{FETCH_WORKFLOW}/dispatches"
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    # We trigger the workflow on the 'main' branch
    data = {
        "ref": "main",
        "inputs": {}
    }
    
    # Add optional target_date input if provided
    if target_date:
        data["inputs"]["target_date"] = target_date
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                # GitHub returns 204 No Content on a successful dispatch
                if response.status == 204:
                    await status_msg.edit(content="💠 **Transmission Successful!**\n> **NewsFetcher** is initializing... Fetching live link... 📡")
                    print(f"Triggered fetch via Discord user: {ctx.author}")
                    
                    # Try up to 3 times with 4s wait each (total 12s)
                    live_url = None
                    for attempt in range(1, 4):
                        await asyncio.sleep(4)
                        print(f"Attempt {attempt} to fetch live link...")
                        
                        runs_url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{FETCH_WORKFLOW}/runs"
                        async with session.get(runs_url, headers=headers) as runs_resp:
                            if runs_resp.status == 200:
                                runs_data = await runs_resp.json()
                                if runs_data.get("workflow_runs"):
                                    live_url = runs_data["workflow_runs"][0]["html_url"]
                                    break
                            else:
                                print(f"Failed to fetch runs on attempt {attempt}: {runs_resp.status}")
                    
                    if live_url:
                        date_note = f" for `{target_date}`" if target_date else ""
                        await status_msg.edit(content=f"💠 **Transmission Successful!**{date_note}\n> **NewsFetcher** is now initializing the background runner.\n> 🔗 **[Watch Live Updates on GitHub](<{live_url}>)**\n\n> A typical run takes **10-15 minutes**. The final report will be delivered here once complete. 📰")
                    else:
                        await status_msg.edit(content="💠 **Transmission Successful!**\n> **NewsFetcher** is now initializing the background runner. (Live link could not be retrieved - check GitHub Actions manually)\n\n> A typical run takes **10-15 minutes**. The final report will be delivered here once complete. 📰")
                else:
                    try:
                        response_json = await response.json()
                        error_details = response_json.get("message", "No error message provided")
                    except:
                        error_details = await response.text()
                    
                    await status_msg.edit(content=f"❌ **Failed to trigger workflow.**\nGitHub API Error ({response.status}): `{error_details}`\n> **Workflow:** `{FETCH_WORKFLOW}`\n> **Repo:** `{GITHUB_REPO}`")
                    print(f"Failed to trigger: {response.status} - {error_details}")
            
    except Exception as e:
        await status_msg.edit(content=f"⚠️ **Internal Error:** Could not reach GitHub.\n`{str(e)}`")
        print(f"Exception triggering workflow: {e}")

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("CRITICAL: DISCORD_BOT_TOKEN is missing.")
        exit(1)
    if not GITHUB_TOKEN:
        print("CRITICAL: GITHUB_PAT is missing.")
        exit(1)
        
    print("Starting bot...")
    bot.run(DISCORD_TOKEN)
