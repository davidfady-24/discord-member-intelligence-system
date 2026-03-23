# export_premium_full.py
"""
Reads YAGPDB embed messages from a #log channel and exports premium_data_full.xlsx
with multiple sheets:
 - All_Members
 - Premium_Subscriptions
 - Summary_Stats
 - Monthly_Income (income grouped by month when role was granted)

Usage: set DISCORD_TOKEN env var (or paste token in TOKEN variable below for local testing).
"""

import re
import json
import os
import tempfile
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import discord
from discord.utils import snowflake_time

# ====== CONFIG ======
TOKEN = os.getenv("DISCORD_TOKEN", "")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
YAGPDB_BOT_ID = int(os.getenv("YAGPDB_BOT_ID", "204255221017214977"))
STATE_FILE = "state_embeds.json"
PRICES_BY_MONTHS = {1: 300, 3: 850, 6: 1550}
OUTPUT_XLSX = "discord_full_export.xlsx"
# =====================

intents = discord.Intents.default()
intents.guilds = True
intents.messages = True
intents.message_content = True
intents.members = True
client = discord.Client(intents=intents)

# Regex helpers
RE_ID_IN_PAR = re.compile(r"\(ID\s*([0-9]{17,19})\)", re.I)
RE_TO_NAME = re.compile(r"to\s+(.+?)(?:\s*\(ID\b|\bfor\b|$)", re.I)
RE_DURATION_FOOTER = re.compile(r"Duration\s*:\s*(.+)", re.I)
RE_DURATION_SIMPLE = re.compile(r"(\d+)\s*(months?|month|weeks?|week|days?|day|w|d|m)\b", re.I)
RE_SNOWFLAKE = re.compile(r"\b([0-9]{17,19})\b")

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    # default state includes subscriptions list for audit/history
    return {"last_message_id": None, "members": {}, "subscriptions": []}

def save_state(s):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(s, f, ensure_ascii=False, indent=2, default=str)

def parse_duration_to_relativedelta(text):
    """Extracts months/weeks/days and converts to relativedelta (30 days = 1 month)."""
    if not text:
        return None
    months = weeks = days = 0
    for m in RE_DURATION_SIMPLE.finditer(text):
        num = int(m.group(1))
        unit = m.group(2).lower()
        if unit.startswith("month") or unit == "m":
            months += num
        elif unit.startswith("week") or unit == "w":
            weeks += num
        elif unit.startswith("day") or unit == "d":
            days += num
    total_days = days + weeks * 7 + months * 30
    real_months = total_days // 30
    rem_days = total_days % 30
    return relativedelta(months=real_months, days=rem_days)

def rd_to_label(rd):
    """Readable label: 30+ days = months, <30 days = days."""
    if not rd:
        return ""
    months = getattr(rd, "months", 0)
    days = getattr(rd, "days", 0)
    total_days = months * 30 + days
    if total_days >= 30:
        months_label = total_days // 30
        return f"{months_label} month{'s' if months_label > 1 else ''}"
    else:
        return f"{total_days} day{'s' if total_days != 1 else ''}"

def months_from_rd(rd):
    """Return integer months from relativedelta approximation (30 days == 1 month)."""
    if not rd:
        return 0
    months = getattr(rd, "months", 0)
    days = getattr(rd, "days", 0)
    total_days = months * 30 + days
    return total_days // 30

def parse_embed_message(embed):
    """Return (discord_id, nickname, relativedelta_duration, raw_duration_text)"""
    desc = embed.description or ""
    dur_text = ""
    if embed.footer and embed.footer.text:
        m = RE_DURATION_FOOTER.search(embed.footer.text)
        if m:
            dur_text = m.group(1).strip()
    if not dur_text:
        m = RE_DURATION_FOOTER.search(desc)
        if m:
            dur_text = m.group(1).strip()

    m_id = RE_ID_IN_PAR.search(desc)
    discord_id = m_id.group(1) if m_id else None

    m_name = RE_TO_NAME.search(desc)
    nickname = m_name.group(1).strip("` ").strip() if m_name else None

    if not discord_id:
        m_sf = RE_SNOWFLAKE.search(desc)
        if m_sf:
            discord_id = m_sf.group(1)

    rd = parse_duration_to_relativedelta(dur_text or desc)
    return discord_id, nickname, rd, (dur_text or "")

async def fetch_all_guild_members(guild):
    """Yield all members of the guild, with robust fallbacks."""
    yielded_any = False
    try:
        async for member in guild.fetch_members(limit=None):
            yielded_any = True
            yield member
    except Exception:
        yielded_any = False

    if not yielded_any:
        try:
            await guild.chunk()
        except Exception:
            pass
        for member in getattr(guild, "members", []) or []:
            yielded_any = True
            yield member

def safe_write_xlsx_with_sheets(writer_fn, path):
    """
    writer_fn should accept a pandas.ExcelWriter and write sheets to it.
    We write to a temp file then atomically replace the final file.
    """
    dirn = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(suffix=".xlsx", dir=dirn)
    os.close(fd)
    try:
        with pd.ExcelWriter(tmp, engine="openpyxl") as writer:
            writer_fn(writer)
        os.replace(tmp, path)
        print("✅ Wrote Excel:", path)
    except PermissionError:
        if os.path.exists(tmp): os.remove(tmp)
        print("⚠️ Close the Excel file before running again.")
    except Exception as e:
        if os.path.exists(tmp): os.remove(tmp)
        print("Error writing Excel:", e)

async def process_channel(guild):
    state = load_state()
    last_id = state.get("last_message_id")
    members = state.get("members", {})
    subscriptions = state.get("subscriptions", [])

    channel = guild.get_channel(CHANNEL_ID)
    msgs = []
    if not channel:
        print("⚠️ Channel not found. Skipping embed scan and proceeding with member export.")
    else:
        after = None
        if last_id:
            try:
                after = snowflake_time(int(last_id))
            except Exception:
                after = None

        print("Fetching messages after:", after)
        async for m in channel.history(limit=None, after=after, oldest_first=True):
            # only care about YAGPDB embeds
            if not m.author or m.author.id != YAGPDB_BOT_ID:
                continue
            if not m.embeds:
                continue
            msgs.append(m)

    print(f"Found {len(msgs)} new YAGPDB embed messages.")
    for m in msgs:
        for emb in m.embeds:
            discord_id, nickname, rd, raw_dur = parse_embed_message(emb)
            if not discord_id and not nickname:
                continue

            start_dt = m.created_at.replace(tzinfo=timezone.utc)
            end_dt = (start_dt + rd) if rd else None

            key = discord_id if discord_id else f"name:{nickname}"
            # ensure member summary entry exists
            members.setdefault(key, {
                "discord_id": discord_id or "",
                "nickname": nickname or "",
                "number_of_subs": 0,
                "package": "",
                "premium_start_iso": "",
                "premium_end_iso": "",
                "total_paid": 0,
                "subs_1m": 0,
                "subs_3m": 0,
                "subs_6m": 0,
                "joined_iso": "",
            })

            # Update summary counters (lifetime)
            members[key]["number_of_subs"] = members[key].get("number_of_subs", 0) + 1
            if nickname:
                members[key]["nickname"] = nickname

            if rd:
                package_label = rd_to_label(rd)
                m_count = months_from_rd(rd)
                price = PRICES_BY_MONTHS.get(m_count, 0)
                members[key]["package"] = package_label
                members[key]["total_paid"] = members[key].get("total_paid", 0) + price
                if m_count == 1:
                    members[key]["subs_1m"] = members[key].get("subs_1m", 0) + 1
                elif m_count == 3:
                    members[key]["subs_3m"] = members[key].get("subs_3m", 0) + 1
                elif m_count == 6:
                    members[key]["subs_6m"] = members[key].get("subs_6m", 0) + 1
            else:
                package_label = ""
                price = 0
                m_count = 0

            # Decide whether to replace premium_start/end in summary (keep latest start)
            prev_start = members[key].get("premium_start_iso")
            replace = True
            if prev_start:
                try:
                    prev_dt = datetime.fromisoformat(prev_start)
                    if prev_dt >= start_dt:
                        replace = False
                except Exception:
                    pass

            if replace:
                members[key]["premium_start_iso"] = start_dt.strftime("%Y-%m-%d")
                members[key]["premium_end_iso"] = end_dt.strftime("%Y-%m-%d") if end_dt else ""
                members[key]["premium_status"] = (
                    "✅ Active" if end_dt and end_dt > datetime.now(timezone.utc)
                    else ("❌ Expired" if end_dt else "Unknown")
                )

            # Append a subscription event to history (one row per subscription)
            subscriptions.append({
                "discord_id": discord_id or "",
                "nickname": nickname or "",
                "sub_start_iso": start_dt.strftime("%Y-%m-%d"),
                "sub_end_iso": end_dt.strftime("%Y-%m-%d") if end_dt else "",
                "duration_raw": raw_dur,
                "duration_label": package_label,
                "months_count": m_count,
                "price_egp": price,
                "source_msg_id": str(m.id),
            })

            # update state progressively
            state["last_message_id"] = str(m.id)
            state["members"] = members
            state["subscriptions"] = subscriptions
            save_state(state)

    # final state update after processing messages
    if msgs:
        state["last_message_id"] = str(msgs[-1].id)
    state["members"] = members
    state["subscriptions"] = subscriptions
    save_state(state)
    print("Processed entries from embeds:", len(subscriptions))

    # Merge in all current guild members so non-premium users are included
    current_member_ids = set()
    current_display_names_lower = set()
    guild_member_map = {}
    async for gm in fetch_all_guild_members(guild):
        try:
            did_str = str(gm.id)
            current_member_ids.add(did_str)
            guild_member_map[did_str] = gm
            display_name = gm.display_name if hasattr(gm, "display_name") else (getattr(gm, "nick", None) or getattr(gm, "name", ""))
            if display_name:
                current_display_names_lower.add(display_name.lower())
        except Exception:
            pass
        key = str(gm.id)
        display_name = gm.display_name if hasattr(gm, "display_name") else (getattr(gm, "nick", None) or getattr(gm, "name", ""))
        if gm.joined_at:
            ja = gm.joined_at
            if ja.tzinfo is not None:
                joined_iso = ja.astimezone(timezone.utc).strftime("%Y-%m-%d")
            else:
                joined_iso = ja.strftime("%Y-%m-%d")
        else:
            joined_iso = ""
        if key not in members:
            members[key] = {
                "discord_id": key,
                "nickname": display_name,
                "number_of_subs": 0,
                "package": "",
                "premium_start_iso": "",
                "premium_end_iso": "",
                "premium_status": "❌ Not premium",
                "total_paid": 0,
                "subs_1m": 0,
                "subs_3m": 0,
                "subs_6m": 0,
                "joined_iso": joined_iso,
            }
        else:
            # Update nickname/joined date if available
            if display_name:
                members[key]["nickname"] = display_name
            if joined_iso and not members[key].get("joined_iso"):
                members[key]["joined_iso"] = joined_iso

    # Prune members who are no longer in guild: keep matched by id or matching display name
    pruned = {}
    for m_key, data in members.items():
        did = (data.get("discord_id") or "").strip()
        nick = (data.get("nickname") or "").strip()
        if did and did in current_member_ids:
            pruned[m_key] = data
        elif m_key.startswith("name:") and nick and nick.lower() in current_display_names_lower:
            pruned[m_key] = data
        # else: remove (user left or unmatched name-only)
    members = pruned

    # Recompute aggregated member-level metrics from subscriptions (defensive)
    subs_df = pd.DataFrame(subscriptions)
    if not subs_df.empty:
        # compute durations in days where possible
        def duration_days(row):
            try:
                if row["sub_end_iso"]:
                    s = datetime.fromisoformat(row["sub_start_iso"])
                    e = datetime.fromisoformat(row["sub_end_iso"])
                    return (e - s).days
            except Exception:
                return None
            return None
        subs_df["duration_days"] = subs_df.apply(duration_days, axis=1)
    else:
        subs_df["duration_days"] = []

    # Build All_Members rows by aggregating subscriptions
    rows = []
    now_utc = datetime.now(timezone.utc)
    for k, v in members.items():
        did = v.get("discord_id", "")
        # find all subscriptions for this discord id
        user_subs = [s for s in subscriptions if s.get("discord_id") == did]
        total_paid = sum(s.get("price_egp", 0) for s in user_subs)
        number_of_subs = len(user_subs) if user_subs else v.get("number_of_subs", 0)
        last_sub = None
        if user_subs:
            # last by start date
            try:
                last_sub = max(user_subs, key=lambda x: x.get("sub_start_iso", ""))
            except Exception:
                last_sub = user_subs[-1]
        # active check: latest sub_end in future?
        active = False
        last_end = ""
        if last_sub and last_sub.get("sub_end_iso"):
            last_end_dt = None
            try:
                last_end_dt = datetime.fromisoformat(last_sub["sub_end_iso"])
                if last_end_dt.tzinfo is None:
                    last_end_dt = last_end_dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass
            if last_end_dt and last_end_dt > now_utc:
                active = True
                last_end = last_end_dt.strftime("%Y-%m-%d")
            elif last_end_dt:
                last_end = last_end_dt.strftime("%Y-%m-%d")

        joined = v.get("joined_iso", "")
        rows.append({
            "Discord ID": did,
            "Nickname": v.get("nickname", ""),
            "Joined": joined,
            "Premium Status": ("✅ Active" if active else (v.get("premium_status", "❌ Not premium"))),
            "Premium Start": v.get("premium_start_iso", ""),
            "Premium End": v.get("premium_end_iso", ""),
            "Number of Subs": number_of_subs,
            "Total Paid": total_paid,
            "1m Subs": v.get("subs_1m", 0),
            "3m Subs": v.get("subs_3m", 0),
            "6m Subs": v.get("subs_6m", 0),
            "Last Sub End": last_end,
            "HistoryCount": len(user_subs),
        })

    # Map of financial metrics per member for reuse in other sheets
    member_financials = {}
    for r in rows:
        did_row = r.get("Discord ID")
        if not did_row:
            continue
        member_financials[did_row] = {
            "total_paid": r.get("Total Paid", 0),
            "subs_count": r.get("Number of Subs", 0),
            "premium_status": r.get("Premium Status", ""),
        }

    all_members_df = pd.DataFrame(rows)
    if not all_members_df.empty:
        all_members_df = all_members_df.sort_values(by=["Number of Subs", "Nickname"], ascending=[False, True])

    # Build Premium_Subscriptions sheet (one row per historical subscription)
    subs_sheet_df = pd.DataFrame(subscriptions)
    if not subs_sheet_df.empty:
        # reorder columns to be readable
        col_order = ["discord_id", "nickname", "sub_start_iso", "sub_end_iso", "duration_label",
                     "duration_raw", "months_count", "duration_days", "price_egp", "source_msg_id"]
        # keep only existing columns in case of empty fields
        col_order = [c for c in col_order if c in subs_sheet_df.columns]
        subs_sheet_df = subs_sheet_df[col_order]
        subs_sheet_df = subs_sheet_df.sort_values(by=["sub_start_iso"], ascending=True)

    # Prepare additional subscription-level datetime fields
    if not subs_df.empty:
        if "sub_start_iso" in subs_df.columns:
            subs_df["sub_start_dt"] = pd.to_datetime(subs_df["sub_start_iso"], errors="coerce")
        else:
            subs_df["sub_start_dt"] = pd.NaT
        if "sub_end_iso" in subs_df.columns:
            subs_df["sub_end_dt"] = pd.to_datetime(subs_df["sub_end_iso"], errors="coerce")
        else:
            subs_df["sub_end_dt"] = pd.NaT
    else:
        subs_df["sub_start_dt"] = []
        subs_df["sub_end_dt"] = []

    # Sets for churn and server membership
    current_member_ids_set = set(current_member_ids)
    ever_premium_ids = set(s.get("discord_id") for s in subscriptions if s.get("discord_id"))
    left_premium_ids = ever_premium_ids - current_member_ids_set

    # Basic revenue metrics
    total_revenue = subs_sheet_df["price_egp"].sum() if (not subs_sheet_df.empty and "price_egp" in subs_sheet_df.columns) else 0
    ever_premium_count = len(ever_premium_ids)
    active_premiums = all_members_df[all_members_df["Premium Status"].str.contains("Active", na=False)].shape[0] if not all_members_df.empty else 0
    expired_premiums = all_members_df[all_members_df["Premium Status"].str.contains("Expired|Not premium", na=False)].shape[0] if not all_members_df.empty else 0
    avg_rev_per_user = (total_revenue / ever_premium_count) if ever_premium_count else 0
    avg_sub_duration = subs_sheet_df["duration_days"].dropna().mean() if (not subs_sheet_df.empty and "duration_days" in subs_sheet_df.columns) else 0

    # retention approximation: users with >1 subscription / users ever premium
    renewals_count = 0
    if not subs_df.empty and "discord_id" in subs_df.columns:
        for _, g in subs_df.groupby("discord_id"):
            if len(g) > 1:
                renewals_count += 1
    retention_rate = (renewals_count / ever_premium_count * 100) if ever_premium_count else 0

    # Most common sub duration
    most_common_duration = ""
    if not subs_sheet_df.empty and "duration_label" in subs_sheet_df.columns:
        duration_series = subs_sheet_df["duration_label"].dropna()
        if not duration_series.empty:
            most_common_duration = duration_series.value_counts().idxmax()

    # Build Monthly_Income sheet (grouped by month when role was granted)
    if not subs_sheet_df.empty and "sub_start_iso" in subs_sheet_df.columns and "price_egp" in subs_sheet_df.columns:
        # Group by year-month from sub_start_iso
        subs_with_month = subs_sheet_df.copy()
        subs_with_month["sub_start_iso"] = pd.to_datetime(subs_with_month["sub_start_iso"], errors="coerce")
        subs_with_month = subs_with_month.dropna(subset=["sub_start_iso"])
        subs_with_month["year_month"] = subs_with_month["sub_start_iso"].dt.to_period("M")

        # Group by year-month and sum prices
        monthly_grouped = subs_with_month.groupby("year_month", as_index=False).agg({
            "price_egp": "sum",
            "discord_id": "count"  # count of subscriptions
        })
        monthly_grouped.columns = ["Year-Month", "Monthly Income (EGP)", "Number of Subscriptions"]
        monthly_grouped["Year-Month"] = monthly_grouped["Year-Month"].astype(str)
        monthly_grouped = monthly_grouped.sort_values(by="Year-Month", ascending=True)
        monthly_income_df = monthly_grouped
    else:
        monthly_income_df = pd.DataFrame(columns=["Year-Month", "Monthly Income (EGP)", "Number of Subscriptions"])

    # Best revenue month
    best_revenue_month_label = ""
    if not monthly_income_df.empty:
        try:
            best_row = monthly_income_df.loc[monthly_income_df["Monthly Income (EGP)"].idxmax()]
            best_revenue_month_label = f"{best_row['Year-Month']} (EGP {best_row['Monthly Income (EGP)']})"
        except Exception:
            best_revenue_month_label = ""

    # Revenue by ISO week
    if not subs_sheet_df.empty and "sub_start_iso" in subs_sheet_df.columns and "price_egp" in subs_sheet_df.columns:
        rev_week_df = subs_sheet_df.copy()
        rev_week_df["sub_start_iso"] = pd.to_datetime(rev_week_df["sub_start_iso"], errors="coerce")
        rev_week_df = rev_week_df.dropna(subset=["sub_start_iso"])
        if not rev_week_df.empty:
            iso = rev_week_df["sub_start_iso"].dt.isocalendar()
            rev_week_df["iso_year"] = iso["year"]
            rev_week_df["iso_week"] = iso["week"]
            rev_week_df["Year-Week"] = rev_week_df["iso_year"].astype(str) + "-W" + rev_week_df["iso_week"].astype(str).str.zfill(2)
            weekly_grouped = rev_week_df.groupby("Year-Week", as_index=False).agg({
                "price_egp": "sum",
                "discord_id": "count"
            })
            weekly_grouped.columns = ["Year-Week", "Weekly Income (EGP)", "Number of Subscriptions"]
            weekly_grouped = weekly_grouped.sort_values(by="Year-Week", ascending=True)
            revenue_by_week_df = weekly_grouped
        else:
            revenue_by_week_df = pd.DataFrame(columns=["Year-Week", "Weekly Income (EGP)", "Number of Subscriptions"])
    else:
        revenue_by_week_df = pd.DataFrame(columns=["Year-Week", "Weekly Income (EGP)", "Number of Subscriptions"])

    best_revenue_week_label = ""
    if not revenue_by_week_df.empty:
        try:
            best_week_row = revenue_by_week_df.loc[revenue_by_week_df["Weekly Income (EGP)"].idxmax()]
            best_revenue_week_label = f"{best_week_row['Year-Week']} (EGP {best_week_row['Weekly Income (EGP)']})"
        except Exception:
            best_revenue_week_label = ""

    # Presence stats (online/offline approx)
    online_count = 0
    offline_count = 0
    for gm in guild_member_map.values():
        try:
            status = getattr(gm, "status", None)
            if str(status) == "online":
                online_count += 1
            else:
                offline_count += 1
        except Exception:
            offline_count += 1

    # Premium role members (any role with 'premium' in name)
    total_active_role_members = 0
    try:
        premium_roles = [r for r in getattr(guild, "roles", []) if "premium" in (getattr(r, "name", "") or "").lower()]
        premium_role_ids = {r.id for r in premium_roles}
        if premium_role_ids:
            for gm in guild_member_map.values():
                roles = getattr(gm, "roles", []) or []
                if any(getattr(role, "id", None) in premium_role_ids for role in roles):
                    total_active_role_members += 1
    except Exception:
        total_active_role_members = 0

    server_total_members = len(guild_member_map) if guild_member_map else len(current_member_ids_set)

    left_premium_count = len(left_premium_ids)

    # Server overview + join counts per month
    join_counts_by_month = defaultdict(int)
    for gm in guild_member_map.values():
        try:
            ja = gm.joined_at
            if not ja:
                continue
            if ja.tzinfo is not None:
                ja = ja.astimezone(timezone.utc)
            ym = ja.strftime("%Y-%m")
            join_counts_by_month[ym] += 1
        except Exception:
            continue

    server_overview_rows = [
        {"Year-Month": "Total Members (current)", "New Members": server_total_members},
        {"Year-Month": "Premium Users Still In Server", "New Members": ever_premium_count - left_premium_count},
        {"Year-Month": "Premium Users Who Left", "New Members": left_premium_count},
        {"Year-Month": "Online Members (approx)", "New Members": online_count},
        {"Year-Month": "Offline/Other Members (approx)", "New Members": offline_count},
    ]
    for ym in sorted(join_counts_by_month.keys()):
        server_overview_rows.append({"Year-Month": ym, "New Members": join_counts_by_month[ym]})
    server_overview_df = pd.DataFrame(server_overview_rows)

    # Daily join counts sheet
    join_counts_by_day = defaultdict(int)
    for gm in guild_member_map.values():
        try:
            ja = gm.joined_at
            if not ja:
                continue
            if ja.tzinfo is not None:
                ja = ja.astimezone(timezone.utc)
            day_str = ja.strftime("%Y-%m-%d")
            join_counts_by_day[day_str] += 1
        except Exception:
            continue
    daily_join_rows = [{"Date": d, "New Members": count} for d, count in sorted(join_counts_by_day.items())]
    daily_joins_df = pd.DataFrame(daily_join_rows)

    # Member_Growth sheet (one row per current member)
    member_growth_rows = []
    for did_str, gm in guild_member_map.items():
        try:
            display_name = gm.display_name if hasattr(gm, "display_name") else (getattr(gm, "nick", None) or getattr(gm, "name", ""))
        except Exception:
            display_name = ""
        # join date
        join_str = ""
        try:
            if gm.joined_at:
                ja = gm.joined_at
                if ja.tzinfo is not None:
                    ja = ja.astimezone(timezone.utc)
                join_str = ja.strftime("%Y-%m-%d")
        except Exception:
            join_str = ""
        # account created
        try:
            created_dt = snowflake_time(gm.id)
            if created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=timezone.utc)
            created_str = created_dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            created_str = ""
        is_bot = bool(getattr(gm, "bot", False))
        roles = []
        try:
            for role in getattr(gm, "roles", []) or []:
                name = getattr(role, "name", None)
                if name and name != "@everyone":
                    roles.append(name)
        except Exception:
            roles = []
        roles_str = ", ".join(sorted(set(roles))) if roles else ""
        fin = member_financials.get(did_str, {})
        premium_status_full = fin.get("premium_status", "")
        if "Active" in premium_status_full:
            premium_status_simple = "Active"
        elif "Expired" in premium_status_full:
            premium_status_simple = "Expired"
        elif fin:
            premium_status_simple = "Expired"
        else:
            premium_status_simple = "Never"
        total_paid_member = fin.get("total_paid", 0)
        subs_count_member = fin.get("subs_count", 0)
        member_growth_rows.append({
            "Discord ID": did_str,
            "Nickname": display_name,
            "Join Date": join_str,
            "Account Created Date": created_str,
            "Is Bot": is_bot,
            "Roles": roles_str,
            "Premium Status": premium_status_simple,
            "Total Paid EGP": total_paid_member,
            "Number of Subs": subs_count_member,
        })
    member_growth_df = pd.DataFrame(member_growth_rows)

    # Role_Stats sheet
    role_member_counts = defaultdict(int)
    for gm in guild_member_map.values():
        for role in getattr(gm, "roles", []) or []:
            role_member_counts[getattr(role, "id", None)] += 1
    role_rows = []
    for role in getattr(guild, "roles", []) or []:
        rid = getattr(role, "id", None)
        if rid is None:
            continue
        member_count = role_member_counts.get(rid, 0)
        color_hex = ""
        try:
            color_val = getattr(getattr(role, "color", None), "value", 0)
            if color_val:
                color_hex = f"#{int(color_val):06X}"
        except Exception:
            color_hex = ""
        role_rows.append({
            "Role Name": getattr(role, "name", ""),
            "Role ID": str(rid),
            "Member Count": member_count,
            "Color": color_hex,
        })
    role_stats_df = pd.DataFrame(role_rows)
    if not role_stats_df.empty:
        role_stats_df = role_stats_df.sort_values(by="Member Count", ascending=False)

    # Churn_Analysis sheet
    churn_rows = []
    for did_str in sorted(left_premium_ids):
        user_subs = [s for s in subscriptions if s.get("discord_id") == did_str]
        if not user_subs:
            continue
        try:
            latest = max(user_subs, key=lambda x: x.get("sub_start_iso", ""))
        except Exception:
            latest = user_subs[-1]
        nickname_latest = latest.get("nickname", "") or ""
        total_paid_left = sum(s.get("price_egp", 0) for s in user_subs)
        subs_count_left = len(user_subs)
        sub_ends = [s.get("sub_end_iso") for s in user_subs if s.get("sub_end_iso")]
        if sub_ends:
            try:
                last_end_left = max(sub_ends)
            except Exception:
                last_end_left = sub_ends[-1]
        else:
            last_end_left = ""
        churn_rows.append({
            "Discord ID": did_str,
            "Nickname": nickname_latest,
            "Last Known Premium End": last_end_left,
            "Total Paid": total_paid_left,
            "Subs Count": subs_count_left,
        })
    churn_df = pd.DataFrame(churn_rows)

    # Top_Members sheet and Retention_Cohorts sheet
    top_members_df = pd.DataFrame()
    retention_cohorts_df = pd.DataFrame()
    if not subs_df.empty and "discord_id" in subs_df.columns:
        # ensure types
        subs_df["discord_id"] = subs_df["discord_id"].astype(str)
        per_user_records = {}
        for did_str, g in subs_df.groupby("discord_id"):
            if not did_str or did_str == "nan":
                continue
            total_paid_user = g["price_egp"].fillna(0).sum() if "price_egp" in g.columns else 0
            subs_count_user = len(g)
            dts = g["sub_start_dt"].dropna().sort_values()
            if not dts.empty:
                first_sub_date = dts.iloc[0].strftime("%Y-%m-%d")
                last_sub_date = dts.iloc[-1].strftime("%Y-%m-%d")
            else:
                first_sub_date = ""
                last_sub_date = ""
            avg_gap = None
            if len(dts) > 1:
                gaps = dts.diff().dropna().dt.days
                if not gaps.empty:
                    avg_gap = gaps.mean()
            nickname_series = g.get("nickname")
            nickname_user = ""
            if nickname_series is not None:
                nickname_series = nickname_series.dropna()
                if not nickname_series.empty:
                    nickname_user = nickname_series.iloc[-1]
            still_in_server = "Yes" if did_str in current_member_ids_set else "No"
            per_user_records[did_str] = {
                "Discord ID": did_str,
                "Nickname": nickname_user,
                "Total Paid": total_paid_user,
                "Subs Count": subs_count_user,
                "First Sub Date": first_sub_date,
                "Last Sub Date": last_sub_date,
                "Avg Days Between Subs": round(avg_gap, 1) if avg_gap is not None else None,
                "Still In Server": still_in_server,
            }
        if per_user_records:
            top_list = sorted(per_user_records.values(), key=lambda x: x["Total Paid"], reverse=True)[:50]
            for idx, rec in enumerate(top_list, start=1):
                rec["Rank"] = idx
            top_members_df = pd.DataFrame(top_list)[
                ["Rank", "Discord ID", "Nickname", "Total Paid", "Subs Count", "First Sub Date", "Last Sub Date", "Avg Days Between Subs", "Still In Server"]
            ]
        # Retention cohorts
        valid = subs_df.dropna(subset=["sub_start_dt"])
        if not valid.empty:
            valid["cohort_month"] = valid.groupby("discord_id")["sub_start_dt"].transform("min").dt.to_period("M")
            valid["start_month"] = valid["sub_start_dt"].dt.to_period("M")
            months_by_user = valid.groupby("discord_id")["start_month"].apply(lambda s: sorted(set(s))).to_dict()
            cohort_pairs = valid[["discord_id", "cohort_month"]].drop_duplicates()
            cohort_rows = []
            for cohort_month, g in cohort_pairs.groupby("cohort_month"):
                cohort_users = list(g["discord_id"])
                total_users_cohort = len(cohort_users)
                m1 = cohort_month + 1
                m2 = cohort_month + 2
                m3 = cohort_month + 3
                renewed_m1 = renewed_m2 = renewed_m3 = 0
                renewed_any = 0
                for uid in cohort_users:
                    user_months = months_by_user.get(uid, [])
                    has_m1 = m1 in user_months
                    has_m2 = m2 in user_months
                    has_m3 = m3 in user_months
                    if has_m1:
                        renewed_m1 += 1
                    if has_m2:
                        renewed_m2 += 1
                    if has_m3:
                        renewed_m3 += 1
                    if has_m1 or has_m2 or has_m3:
                        renewed_any += 1
                retention_pct = (renewed_any / total_users_cohort * 100) if total_users_cohort else 0
                cohort_rows.append({
                    "Cohort Month": str(cohort_month),
                    "Total Users": total_users_cohort,
                    "Renewed M+1": renewed_m1,
                    "Renewed M+2": renewed_m2,
                    "Renewed M+3": renewed_m3,
                    "Retention %": round(retention_pct, 2),
                })
            if cohort_rows:
                retention_cohorts_df = pd.DataFrame(cohort_rows).sort_values(by="Cohort Month")

    # Premium role grant/remove events (approximated from subscription logs)
    premium_events_rows = []
    for sub in subscriptions:
        premium_events_rows.append({
            "Discord ID": sub.get("discord_id") or "",
            "Nickname": sub.get("nickname") or "",
            "Event Type": "Grant",
            "Event Time": sub.get("sub_start_iso") or "",
            "Premium End": sub.get("sub_end_iso") or "",
            "Duration Label": sub.get("duration_label") or "",
            "Price EGP": sub.get("price_egp", 0),
            "Source Message ID": sub.get("source_msg_id") or "",
        })
    premium_events_df = pd.DataFrame(premium_events_rows)

    # Message activity and channel activity for last 30 days
    member_msg_counts = defaultdict(int)
    member_last_msg = {}
    member_channels = defaultdict(set)
    channel_msg_counts = defaultdict(int)
    channel_user_sets = defaultdict(set)
    channel_hour_counts = defaultdict(lambda: defaultdict(int))
    global_hour_counts = defaultdict(int)
    cutoff_dt = now_utc - timedelta(days=30)

    for channel in getattr(guild, "text_channels", []) or []:
        try:
            async for msg in channel.history(limit=None, after=cutoff_dt, oldest_first=False):
                author = getattr(msg, "author", None)
                if not author or getattr(author, "bot", False):
                    continue
                did_msg = str(getattr(author, "id", ""))
                if not did_msg:
                    continue
                ts = getattr(msg, "created_at", None)
                if not ts:
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                local_ts = ts.astimezone()
                hour = local_ts.hour
                member_msg_counts[did_msg] += 1
                if did_msg not in member_last_msg or ts > member_last_msg[did_msg]:
                    member_last_msg[did_msg] = ts
                member_channels[did_msg].add(getattr(channel, "name", str(getattr(channel, "id", ""))))
                cid = getattr(channel, "id", None)
                if cid is not None:
                    channel_msg_counts[cid] += 1
                    channel_user_sets[cid].add(did_msg)
                    channel_hour_counts[cid][hour] += 1
                global_hour_counts[hour] += 1
        except Exception:
            continue

    # Member message activity sheet
    member_activity_rows = []
    for did_msg, count in member_msg_counts.items():
        gm = guild_member_map.get(did_msg)
        nickname = ""
        try:
            if gm:
                nickname = gm.display_name if hasattr(gm, "display_name") else (getattr(gm, "nick", None) or getattr(gm, "name", ""))
            else:
                if not all_members_df.empty:
                    match = all_members_df[all_members_df["Discord ID"] == did_msg]
                    if not match.empty:
                        nickname = match.iloc[0].get("Nickname", "")
        except Exception:
            nickname = ""
        last_ts = member_last_msg.get(did_msg)
        if last_ts:
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=timezone.utc)
            last_str = last_ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
        else:
            last_str = ""
        channels_str = ", ".join(sorted(member_channels.get(did_msg, [])))
        member_activity_rows.append({
            "Member ID": did_msg,
            "Nickname": nickname,
            "Last Message Date": last_str,
            "Total Messages (Last 30 Days)": count,
            "Channels They Post In": channels_str,
        })
    member_activity_df = pd.DataFrame(member_activity_rows)
    if not member_activity_df.empty:
        member_activity_df = member_activity_df.sort_values(by="Total Messages (Last 30 Days)", ascending=False)

    # Channel activity sheet
    channel_activity_rows = []
    for channel in getattr(guild, "text_channels", []) or []:
        cid = getattr(channel, "id", None)
        if cid is None:
            continue
        msg_count = channel_msg_counts.get(cid, 0)
        unique_users = len(channel_user_sets.get(cid, set()))
        hour_dict = channel_hour_counts.get(cid, {})
        most_active_hour = None
        if hour_dict:
            try:
                most_active_hour = max(hour_dict, key=hour_dict.get)
            except Exception:
                most_active_hour = None
        channel_activity_rows.append({
            "Channel ID": str(cid),
            "Channel Name": getattr(channel, "name", ""),
            "Messages Last 30 Days": msg_count,
            "Unique Users Last 30 Days": unique_users,
            "Most Active Hour": most_active_hour,
        })
    channel_activity_df = pd.DataFrame(channel_activity_rows)
    if not channel_activity_df.empty:
        channel_activity_df = channel_activity_df.sort_values(by="Messages Last 30 Days", ascending=False)

    # Peak activity hours sheet (global)
    peak_rows = []
    for hour in range(24):
        peak_rows.append({
            "Hour": hour,
            "Messages Last 30 Days": global_hour_counts.get(hour, 0),
        })
    peak_hours_df = pd.DataFrame(peak_rows)

    # Summary KPIs (including new metrics)
    summary = [
        {"Metric": "Total Revenue (EGP)", "Value": total_revenue},
        {"Metric": "Ever Premium Users", "Value": ever_premium_count},
        {"Metric": "Active Premiums", "Value": active_premiums},
        {"Metric": "Expired / Not Premium (rows)", "Value": expired_premiums},
        {"Metric": "Avg Revenue per Premium User (EGP)", "Value": round(avg_rev_per_user, 2)},
        {"Metric": "Avg Sub Duration (days)", "Value": round(avg_sub_duration, 1)},
        {"Metric": "Users with >1 Sub (renewals)", "Value": renewals_count},
        {"Metric": "Simple Retention Rate (%)", "Value": round(retention_rate, 2)},
        {"Metric": "Total Subscriptions (rows)", "Value": len(subscriptions)},
        {"Metric": "Members Who Left After Subscribing (churn count)", "Value": left_premium_count},
        {"Metric": "Most Common Sub Duration", "Value": most_common_duration},
        {"Metric": "Best Revenue Month", "Value": best_revenue_month_label},
        {"Metric": "Best Revenue Week", "Value": best_revenue_week_label},
        {"Metric": "Total Active Role Members (any premium role)", "Value": total_active_role_members},
        {"Metric": "Server Total Members", "Value": server_total_members},
        {"Metric": "Excel Generated", "Value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")},
    ]
    summary_df = pd.DataFrame(summary)

    # write all sheets to Excel atomically
    def writer_fn(writer):
        # Existing sheets
        if not all_members_df.empty:
            all_members_df.to_excel(writer, sheet_name="All_Members", index=False)
        else:
            pd.DataFrame(columns=["Discord ID", "Nickname"]).to_excel(writer, sheet_name="All_Members", index=False)

        if not subs_sheet_df.empty:
            subs_sheet_df.to_excel(writer, sheet_name="Premium_Subscriptions", index=False)
        else:
            pd.DataFrame(columns=["discord_id", "nickname"]).to_excel(writer, sheet_name="Premium_Subscriptions", index=False)

        summary_df.to_excel(writer, sheet_name="Summary_Stats", index=False)

        if not monthly_income_df.empty:
            monthly_income_df.to_excel(writer, sheet_name="Monthly_Income", index=False)
        else:
            pd.DataFrame(columns=["Year-Month", "Monthly Income (EGP)", "Number of Subscriptions"]).to_excel(
                writer, sheet_name="Monthly_Income", index=False
            )

        # New analytics sheets
        if not server_overview_df.empty:
            server_overview_df.to_excel(writer, sheet_name="Server_Overview", index=False)
        else:
            pd.DataFrame(columns=["Year-Month", "New Members"]).to_excel(writer, sheet_name="Server_Overview", index=False)

        if not member_growth_df.empty:
            member_growth_df.to_excel(writer, sheet_name="Member_Growth", index=False)
        else:
            pd.DataFrame(
                columns=[
                    "Discord ID",
                    "Nickname",
                    "Join Date",
                    "Account Created Date",
                    "Is Bot",
                    "Roles",
                    "Premium Status",
                    "Total Paid EGP",
                    "Number of Subs",
                ]
            ).to_excel(writer, sheet_name="Member_Growth", index=False)

        if not role_stats_df.empty:
            role_stats_df.to_excel(writer, sheet_name="Role_Stats", index=False)
        else:
            pd.DataFrame(columns=["Role Name", "Role ID", "Member Count", "Color"]).to_excel(
                writer, sheet_name="Role_Stats", index=False
            )

        if not churn_df.empty:
            churn_df.to_excel(writer, sheet_name="Churn_Analysis", index=False)
        else:
            pd.DataFrame(
                columns=["Discord ID", "Nickname", "Last Known Premium End", "Total Paid", "Subs Count"]
            ).to_excel(writer, sheet_name="Churn_Analysis", index=False)

        if not revenue_by_week_df.empty:
            revenue_by_week_df.to_excel(writer, sheet_name="Revenue_By_Week", index=False)
        else:
            pd.DataFrame(
                columns=["Year-Week", "Weekly Income (EGP)", "Number of Subscriptions"]
            ).to_excel(writer, sheet_name="Revenue_By_Week", index=False)

        if not top_members_df.empty:
            top_members_df.to_excel(writer, sheet_name="Top_Members", index=False)
        else:
            pd.DataFrame(
                columns=[
                    "Rank",
                    "Discord ID",
                    "Nickname",
                    "Total Paid",
                    "Subs Count",
                    "First Sub Date",
                    "Last Sub Date",
                    "Avg Days Between Subs",
                    "Still In Server",
                ]
            ).to_excel(writer, sheet_name="Top_Members", index=False)

        if not retention_cohorts_df.empty:
            retention_cohorts_df.to_excel(writer, sheet_name="Retention_Cohorts", index=False)
        else:
            pd.DataFrame(
                columns=["Cohort Month", "Total Users", "Renewed M+1", "Renewed M+2", "Renewed M+3", "Retention %"]
            ).to_excel(writer, sheet_name="Retention_Cohorts", index=False)

        if not member_activity_df.empty:
            member_activity_df.to_excel(writer, sheet_name="Member_Message_Activity", index=False)
        else:
            pd.DataFrame(
                columns=[
                    "Member ID",
                    "Nickname",
                    "Last Message Date",
                    "Total Messages (Last 30 Days)",
                    "Channels They Post In",
                ]
            ).to_excel(writer, sheet_name="Member_Message_Activity", index=False)

        if not channel_activity_df.empty:
            channel_activity_df.to_excel(writer, sheet_name="Channel_Activity", index=False)
        else:
            pd.DataFrame(
                columns=[
                    "Channel ID",
                    "Channel Name",
                    "Messages Last 30 Days",
                    "Unique Users Last 30 Days",
                    "Most Active Hour",
                ]
            ).to_excel(writer, sheet_name="Channel_Activity", index=False)

        if not daily_joins_df.empty:
            daily_joins_df.to_excel(writer, sheet_name="Daily_Join_Counts", index=False)
        else:
            pd.DataFrame(columns=["Date", "New Members"]).to_excel(
                writer, sheet_name="Daily_Join_Counts", index=False
            )

        if not peak_hours_df.empty:
            peak_hours_df.to_excel(writer, sheet_name="Peak_Activity_Hours", index=False)
        else:
            pd.DataFrame(columns=["Hour", "Messages Last 30 Days"]).to_excel(
                writer, sheet_name="Peak_Activity_Hours", index=False
            )

        if not premium_events_df.empty:
            premium_events_df.to_excel(writer, sheet_name="Premium_Role_Events", index=False)
        else:
            pd.DataFrame(
                columns=[
                    "Discord ID",
                    "Nickname",
                    "Event Type",
                    "Event Time",
                    "Premium End",
                    "Duration Label",
                    "Price EGP",
                    "Source Message ID",
                ]
            ).to_excel(writer, sheet_name="Premium_Role_Events", index=False)

    safe_write_xlsx_with_sheets(writer_fn, os.path.join(os.path.dirname(__file__) or ".", OUTPUT_XLSX))
    print("Done. Sheets: All_Members, Premium_Subscriptions, Summary_Stats, Monthly_Income")

@client.event
async def on_ready():
    print(f"✅ Logged in as {client.user} ({client.user.id})")
    guild = client.get_guild(GUILD_ID)
    if not guild:
        print("❌ Wrong GUILD_ID or bot not in server.")
        await client.close()
        return
    await process_channel(guild)
    await client.close()

if __name__ == "__main__":
    if not TOKEN or TOKEN.startswith("<paste"):
        raise SystemExit("DISCORD_TOKEN environment variable is missing or placeholder. Set it and re-run.")
    try:
        client.run(TOKEN)
    except discord.errors.LoginFailure:
        raise SystemExit("Login failed. Check DISCORD_TOKEN value and bot permissions.")
