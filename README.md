# Discord Member Intelligence System

Automated Python bot that scrapes a private Discord server's log channel,
parses subscription events, and compiles a full member database into a
structured multi-sheet Excel report — end-to-end automated.

## What it does
- Fetches 3,500+ guild members via async Discord client with fallback chunking
- Parses YAGPDB role-assignment embeds using regex to extract subscription events
- Tracks every subscription lifecycle: start, end, renewal, duration
- Resolves member identity across nickname changes
- Exports 13 production-grade Excel sheets including:
  `All_Members` · `Premium_Subscriptions` · `Summary_Stats` ·
  `Monthly_Income` · `Churn_Analysis` · `Retention_Cohorts` · `Top_Members`
- Incremental snowflake-based checkpoint — re-runs only scan new messages
- Atomic Excel write via temp-file swap (prevents corrupt output)

## Results from production
| Metric | Value |
|--------|-------|
| Members tracked | 3,500+ |
| Subscriptions logged | 370 |
| Total revenue tracked | 141,350 EGP |
| Subscription uplift | +30% after pricing strategy change |
| Retention rate | 34.4% |
| Avg revenue per user | 579 EGP |

## Setup
1. Clone the repo
2. Copy `.env.example` to `.env` and fill in your values
3. Install dependencies: `pip install discord.py pandas openpyxl python-dateutil`
4. Run: `python export_premium_daily.py`

## Tech stack
Python · discord.py · Pandas · Regex · openpyxl · JSON · dotenv
