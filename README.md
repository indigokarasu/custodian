# custodian

<p align="center">
  <img src="./assets/readme/hero.jpg" width="100%" alt="Custodian: autonomous operations monitor for Hermes Agent — gateway logs, cron jobs, skill journals, OCAS data directories">
</p>

Custodian is an operations monitoring and auto-repair plugin for Hermes Agent. It watches gateway logs, cron job health, skill journals, and OCAS data directories. When something fails, it classifies the issue, attempts Tier 1 fixes during quiet hours, and escalates when confidence is low.

**Capabilities:**
- Lifecycle hooks: `post_tool_call`, `on_session_start`, `on_session_end`, `on_session_reset`
- Tools: `custodian_status`, `custodian_scan`, `custodian_issues`
- Slash commands: `/custodian status`, `/custodian scan light`, `/custodian scan deep`, `/custodian issues list`, `/custodian repair auto`, `/custodian schedule show`
