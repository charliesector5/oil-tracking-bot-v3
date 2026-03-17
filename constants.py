HELP_TEXT = """\
Available commands:

General
/start - bot status
/help - show this help
/ping - quick bot check
/checksheet - verify Google Sheet connectivity
/sheetinfo - show connected worksheet title

User Commands
/history - view your recent OIL records
/summary - view your OIL summary
/clockoff - clock normal OIL
/claimoff - claim normal OIL
/clockphoff - clock PH OIL
/claimphoff - claim PH OIL
/clockspecialoff - clock Special OIL
/claimspecialoff - claim Special OIL
/newuser - import old OIL records for a brand-new user

Admin Commands
/startadmin - start admin PM session
/overview - view sector OIL overview
/detailedoverview - view detailed sector OIL overview
/adjustoil - manually adjust one user's OIL
/massadjustoff - mass adjust OIL for all tracked users

Important Notes
- Claim commands will show your current available balance first.
- PH and Special claims cannot go below available active balance.
- Normal OIL may go negative and will be flagged to admin where applicable.
- Use -quit anytime during an active flow to cancel.

Onboarding Notes
- /newuser is for brand-new onboarding / import only.
- During PH and Special onboarding, entries must be keyed in using FIFO order.
- This means you must enter PH / Special from the oldest date to the newest date.
- In practice, key in the earliest expiry first.
- If you enter a later date first and then an earlier date later, the bot will reject it.
- You may use the redo buttons to restart just the PH section or Special section if needed.
"""
