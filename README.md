# Discord Member Intelligence System

An automated Python bot that scrapes a private Discord server's log channel,
parses subscription events, and compiles a full member database into a
structured multi-sheet Excel report — completely end-to-end automated.

## What it does
- Fetches 3,500+ guild members via async Discord client with fallback chunking
- Parses YAGPDB role-assignment embeds using regex to extract subscription events
- Tracks every subscription lifecycle: start, end, renewal, duration
- Resolves member identity across nickname changes
- Exports 4 production-grade Excel sheets:
  - All_Members · Premium_Subscriptions · Summary_Stats · Monthly_Income
- Incremental snowflake-based checkpoint system — re-runs only scan new messages

## Results from production data
| Metric | Value |
|--------|-------|
| Members fetched | 3,500+ |
| Subscriptions logged | 370 |
| Total revenue tracked | 141,350 EGP |
| Subscription uplift | +30% after pricing strategy change |
| Retention rate | 34.4% |

## Technical highlights
- Async Discord client with chunking strategy for large servers
- Regex parsing of embed descriptions → Discord IDs, nicknames, duration text
- `relativedelta` duration engine: converts "3 weeks and 2 days" → exact EGP pricing
- Atomic Excel write via temp-file swap — prevents corrupt output on interruption
- JSON checkpoint file for incremental state management

## Tools
Python · discord.py · Pandas · Regex · openpyxl · JSON · Excel

## Sample output
![Sample Report](output_samples/sample_report.png)
