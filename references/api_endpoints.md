# API Endpoints & Auth Methods

Service-specific test endpoints and authentication methods used by the API key audit workflow.

| Service | Test Endpoint | Method | Expected |
|---------|---------------|--------|----------|
| Alpaca | `/v2/account` | GET with Basic auth (key_id:secret) | `{"id": "..."}` |
| ElevenLabs | `/v1/voices` | GET with `xi-api-key` header | `{"voices": [...]}` |
| Hunter.io | `/v2/account?api_key=KEY` | GET | `{"data": {"plan_name": ...}}` |
| Fal.ai | `/v1/models` via queue API | GET with `Authorization: Key KEY` | Model list or error detail |
| Firecrawl | `/v1/scrape` POST | POST with Bearer token | `{"success": true}` or `{"error": "..."}` |
| Google | People API via OAuth | Python `googleapiclient` | Contact count |
| GitHub | `gh api user` | CLI or `curl -H "Authorization: token KEY"` | `{"login": "..."}` |
| NVIDIA | `/v1/models` | GET with Bearer token | `{"data": [...]}` |
| OpenRouter | `/api/v1/auth/key` | GET with Bearer token | `{"data": {"label": ...}}` |
| Trello | `/1/members/me?key=KEY&token=TOKEN` | GET | `{"id": "..."}` |
| Twilio | `/2010-04-01/Accounts/SID.json` | GET with Basic auth (SID:TOKEN) | `{"sid": "..."}` |
| Spotify | `/api/token` with client_credentials | POST with Base64 client_id:secret | `{"access_token": "..."}` |
| Mem0 | `/v1/memories/` | GET with `Api-Key` header | JSON response |
| Atlassian | `/me` | GET with Bearer token | `{"account_id": "..."}` |
| OpenCode Go | `ollama.com/v1/models` | GET with Bearer token (from config.yaml) | Model list |
| GLM / z.ai | Zhipu requires JWT-signed tokens, NOT raw Bearer. Raw Bearer auth returns 401 on both `api.z.ai` and `open.bigmodel.cn`. Only the SDK or JWT generation works. | — | — |
| Telegram Bot | Active gateway is proof of validity. Can also `getMe` via API. | — | — |

## Provider-key location notes

LLM provider keys often live in `config.yaml` under `model.api_key`, NOT in `.env`:

- **OpenCode Go / GLM** — Key is in `~/.hermes/config.yaml` at `model.api_key`. Base URL is `ollama.com/v1` (OpenCode Go endpoint), NOT the raw zhipuai endpoint.
- **OpenRouter** — Keys in `.env` as `OPENROUTER_API_KEY`, `OPENROUTER_API_KEY_2`, `OPENROUTER_API_KEY_3`, etc. The primary key may be missing (commented out) with only suffixed keys active.
- **NVIDIA** — May have multiple keys (`NVIDIA_API_KEY`, `NVIDIA_API_KEY_2`). Both need testing independently.

## Alternate auth methods (do NOT flag as broken)

- **CLI auth** (`gh auth login`) — Stored in `~/.config/gh/hosts.yml`. Test with `gh api user`.
- **Local services** (Ollama, SearXNG) — Running on localhost, key optional. Test with `curl localhost:PORT/api/tags`.
- **No key needed** — Some services in `.env` are config-only (booleans, URLs, numbers). Skip these.

## Pitfalls

- **Don't confuse `# KEY=***` with a comment.** In `.env` files, a `#`-prefixed line is a comment/placeholder. But some keys (like `GITHUB_TOKEN`) may have been set to `***` as a redacted placeholder that the user expects you to replace. Ask before deleting.
- **Don't false-positive alternate auth services.** If Google API key shows as `***` or expired, check if OAuth tokens exist before calling it "broken."
- **Google OAuth tokens expire.** Always try `creds.refresh(Request())` before declaring Google auth broken.
- **Firecrawl may return HTML instead of JSON.** Use `/v1/scrape` POST endpoint, not `/v1/team`.
- **Mem0 uses `Api-Key` header**, not `Authorization: Token`. The header name matters.
- **`gh auth login` and `.env GITHUB_TOKEN` are separate.** `gh` stores credentials in `~/.config/gh/hosts.yml` independently. If both exist, `GH_TOKEN` env var takes precedence but `gh auth` is more reliable for CLI use.
- **Don't leave `***` in `.env`.** Either replace with a real key or remove the line. Placeholders cause confusion about whether a key exists.
- **GLM / z.ai keys use JWT auth, not Bearer.** A raw `Authorization: Bearer <id.secret>` call to zhipu endpoints returns 401. The key format `id.secret` must be signed into a JWT using HMAC-SHA256. However, the actual provider endpoint used by the agent is often OpenCode Go (`ollama.com/v1`) which DOES accept Bearer auth. Test the actual base_url from config, not the documentation URL.
- **OpenCode Go endpoint is `ollama.com/v1`, not `opencode.ai`.** The `model.api_key` in config.yaml with `base_url: https://ollama.com/v1` is OpenCode Go, not a local Ollama instance.
- **Multiple suffixed keys exist.** `OPENROUTER_API_KEY_2`, `NVIDIA_API_KEY_2`, etc. Scan for numeric suffixes, don't just look for the base name.
- **Google OAuth scope errors.** When refreshing Google OAuth tokens, don't hardcode scopes — read them from the token file itself (`token_data.get("scopes")`) or pass `None` to Credentials to use stored scopes. Hardcoded scopes cause `invalid_scope` errors.
- **`curl -w '%{http_code}'` syntax matters.** In execute_code heredocs, Python f-strings can eat the curly braces. Use `%%{http_code}` or Python urllib instead for reliability.
- **LadybugDB queries use Connection, not raw SQL.** For Weave operations, use `real_ladybug` Python module with `lb.Database()` and `lb.Connection()`.
- **Sync Weave → Google Contacts** using Google Contacts API with OAuth.
