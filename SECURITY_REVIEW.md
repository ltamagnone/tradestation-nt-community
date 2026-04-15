# Security Review: tradestation-nt-community

**Date:** 2026-04-14
**Scope:** Full review of the `tradestation_nt_community` package for injection vulnerabilities, authentication/authorization flaws, credential handling, and insecure data handling.

---

## HIGH Severity

### 1. API response bodies leaked in exception messages -- FIXED

**Status:** Resolved.

All 13 error sites in `http/client.py` now log the full `response.text` at DEBUG level (truncated to 500 chars) and raise exceptions with only the HTTP status code.

Order-execution endpoints (place, replace, cancel, place_group) additionally include a truncated (200 char) response excerpt in the exception message. This is required because `execution.py` parses exception text to detect specific broker conditions (e.g. `"Not an open order"` for expired DAY orders on cancel).

---

### 2. Credentials stored as public attributes with no cleanup

**File:** `http/client.py` lines 47-49, 66-67

```python
self.client_id = client_id or os.getenv("TRADESTATION_CLIENT_ID")
self.client_secret = client_secret or os.getenv("TRADESTATION_CLIENT_SECRET")
self.refresh_token = refresh_token or os.getenv("TRADESTATION_REFRESH_TOKEN")
...
self.access_token: str | None = None
```

All five credential fields are public instance attributes. They persist for the entire process lifetime. The `close()` method (line 506-508) only closes the httpx client -- it does not clear credentials.

Additionally, `factories.py:21-58` caches these credentials in `@lru_cache(1)` function arguments, creating a second copy that is never clearable.

**Risk:** Any code with a reference to the client object (or any heap dump / core file) exposes all credentials in plaintext. The `lru_cache` makes them immune to garbage collection.

**Fix:**

- Use underscore-prefixed private attributes (`self._client_secret`).
- Clear credentials in `close()`:

```python
async def close(self) -> None:
    self.access_token = None
    self._client_secret = None
    self._refresh_token = None
    await self._httpx.aclose()
```

- Consider passing a credential-provider callable instead of raw strings to the factory cache.

---

### 3. Unvalidated `base_url` override enables credential theft

**Files:** `config.py:47,98`, `http/client.py:59-60`, `factories.py:144,214`

```python
# config.py
base_url_http: str | None = None  # lines 47, 98

# http/client.py
if base_url:
    self.base_url = base_url  # line 60 -- no validation
```

Any string is accepted. The OAuth token, `client_id`, and `client_secret` are then sent to whatever URL is provided -- including `http://` (unencrypted), localhost, or attacker-controlled servers.

**Risk:** If config is loaded from a file that can be tampered with, or if the value is sourced from an environment variable that an attacker controls, all credentials and trading operations are redirected.

**Fix:** Validate the scheme is HTTPS and the hostname matches a known TradeStation domain:

```python
_ALLOWED_HOSTS = {"api.tradestation.com", "sim-api.tradestation.com", "signin.tradestation.com"}

if base_url:
    parsed = urllib.parse.urlparse(base_url)
    if parsed.scheme != "https" or parsed.hostname not in _ALLOWED_HOSTS:
        raise ValueError(f"base_url must be HTTPS to a TradeStation domain, got: {base_url}")
    self.base_url = base_url
```

---

## MEDIUM Severity

### 4. Query string injection in `stream_bars()`

**File:** `streaming/client.py` lines 179-183

```python
url = f"{self._base_url}/marketdata/stream/barcharts/{symbol}"
params = f"?interval={interval}&unit={unit}&barsback=1"
if session_template:
    params += f"&sessiontemplate={session_template}"
async for event in self._stream(url + params):
```

Parameters are manually concatenated into the query string without URL encoding. Compare with `http/client.py:141-150` which correctly uses httpx's `params` dict.

**Risk:** A `session_template` value like `"USEQPreAndPost&malicious=true"` injects extra query parameters. A `symbol` value with `/` or `?` characters could alter the URL path or query structure.

**Fix:** Use httpx's parameter handling consistently:

```python
url = f"{self._base_url}/marketdata/stream/barcharts/{symbol}"
params = {"interval": interval, "unit": unit, "barsback": "1"}
if session_template:
    params["sessiontemplate"] = session_template
# Pass params to the stream method instead of concatenating
```

---

### 5. User-supplied values interpolated into URL paths without encoding

**File:** `http/client.py` -- 10 endpoints; `streaming/client.py` -- 4 endpoints

All URL paths use f-string interpolation of user-provided values:

```python
url = f"{self.base_url}/marketdata/barcharts/{symbol}"           # line 140
url = f"{self.base_url}/marketdata/symbols/search/{search_text}" # line 176
url = f"{self.base_url}/brokerage/accounts/{account_keys}/..."   # lines 246, 271, 435
url = f"{self.base_url}/orderexecution/orders/{order_id}"        # lines 369, 405
url = f"{self.base_url}/marketdata/quotes/{symbols}"             # line 499
```

**Risk:** In practice, these values come from NautilusTrader internals (instrument IDs, venue order IDs) -- not raw user input. The practical risk is low because the attack surface is limited to a developer misconfiguring symbol names. However, a `symbol` containing `../` or `?` characters could alter the request path. httpx does not automatically encode path components in pre-built URL strings.

**Fix:** Apply `urllib.parse.quote()` to path segments:

```python
from urllib.parse import quote
url = f"{self.base_url}/marketdata/barcharts/{quote(symbol, safe='')}"
```

---

### 6. SSE streaming client disables timeouts entirely

**File:** `streaming/client.py` line 91

```python
async with httpx.AsyncClient(timeout=None) as client:
```

`timeout=None` disables all timeouts (connect, read, write, pool).

**Risk:** A misbehaving or compromised server that sends data extremely slowly (slowloris-style) holds the connection and its resources indefinitely. With multiple subscriptions, this could exhaust file descriptors or memory.

**Fix:** Use a generous but bounded timeout. SSE connections are long-lived, so disable the read timeout but keep connect/write timeouts:

```python
async with httpx.AsyncClient(
    timeout=httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0)
) as client:
```

---

### 7. Account ID embedded in logged SSE URLs

**File:** `streaming/client.py` lines 96, 103, 123, 126

```python
_log.info(f"SSE stream connected: {url}")             # line 103
_log.error(f"SSE stream error ({url}): {e} ...")       # line 126
```

For order streams, the URL contains the account ID: `.../accounts/{account_id}/orders`.

**Risk:** Log files become a source of account enumeration if compromised.

**Fix:** Mask the account ID in log output, or log only the endpoint path without the account ID.

---

## LOW Severity

### 8. Deprecated `datetime.utcnow()`

**File:** `http/client.py` lines 82, 98

```python
if datetime.utcnow() >= self.token_expiry - timedelta(minutes=5):
```

`datetime.utcnow()` returns a naive datetime and is deprecated since Python 3.12. Not a security bug on its own, but could cause token expiry miscalculation if the system timezone is changed while running.

**Fix:** Use `datetime.now(datetime.UTC)`.

---

### 9. No response size bounds on JSON parsing

**File:** `http/client.py` -- all `response.json()` calls

No `max_content_length` or size check before calling `response.json()`. A malicious or misconfigured API response with a multi-gigabyte JSON body would be deserialized fully into memory.

**Risk:** Low in practice (connecting to TradeStation's own servers), but relevant if `base_url` is overridden (see finding #3).

---

## Positive Findings

- **No hardcoded credentials** anywhere in source, tests, or examples.
- **`.gitignore` properly excludes `.env`** files.
- **TLS enforced** -- all default URLs use HTTPS; `verify=True` is httpx's default.
- **No dangerous deserialization** -- no `eval()`, `exec()`, `pickle`, `subprocess`, or `yaml.load()`.
- **Order bodies use `json=` parameter** -- properly JSON-encoded, no string interpolation in request bodies.
- **Test fixtures use mock values** (`"mock_client_id"`, `"test_client_id"`) -- no real credentials.
- **SSE reconnect has exponential backoff with cap** (8x initial delay) -- prevents reconnect storms.
- **httpx defaults to `follow_redirects=False`** -- mitigates SSRF via redirect chains.
- **No data written to disk** -- all credential and trading data is in-memory only.

---

## Summary

| # | Finding | Severity | Location |
|---|---------|----------|----------|
| 1 | Raw `response.text` in exceptions / logs | **HIGH** | `http/client.py` (13 locations) |
| 2 | Credentials as public attrs, no cleanup | **HIGH** | `http/client.py:47-49,66`, `factories.py:21` |
| 3 | Unvalidated `base_url` enables credential theft | **HIGH** | `config.py:47,98`, `http/client.py:59` |
| 4 | Query string injection in `stream_bars()` | **MEDIUM** | `streaming/client.py:179-183` |
| 5 | URL path segments not encoded | **MEDIUM** | `http/client.py` (10 sites), `streaming/client.py` (4 sites) |
| 6 | SSE client `timeout=None` | **MEDIUM** | `streaming/client.py:91` |
| 7 | Account ID in log messages | **MEDIUM** | `streaming/client.py:96,103,123,126` |
| 8 | Deprecated `datetime.utcnow()` | **LOW** | `http/client.py:82,98` |
| 9 | No response size bounds | **LOW** | `http/client.py` (all `.json()` calls) |
