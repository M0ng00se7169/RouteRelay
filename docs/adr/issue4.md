# Issue #4 — Telegram Integration Not Wired

**File:** `app/infrastructure/integrations/notifications/clients/telegram.py`
**Severity:** Medium (dead code)
**Impact:** The Telegram notification client exists but is never instantiated or used.

> **✅ Implementation Status (2026-09-10):** DONE — Steps 1–4 implemented, full test
> suite green (137 passed). The client is registered in the DI container when
> `TELEGRAM_BOT_TOKEN` is set; when unconfigured, `BaseNotificationClient` stays
> unregistered and `ListenerAddedEventHandler` skips notifications (no error).
> Bonus fix required during implementation: `ListenerAddedEvent` now carries
> `chat_oid` (the handler reads it, but the event previously only had
> `listener_oid`, so notifications could never send). Remaining: Step 5 docs —
> `.env.example` does not exist yet and the README config table lacks the
> `TELEGRAM_*` variables.

## Implementation Plan

### Step 1: Register TelegramNotificationClient in DI Container ✅
*   **File:** `app/logic/init.py`
*   **Actions:**
    1.  Add import for `TelegramNotificationClient` and `BaseNotificationClient`.
    2.  Create a factory function `init_telegram_notification_client()` that:
        *   Resolves Config to get `telegram_bot_token`, `telegram_chat_id`, `telegram_api_url`.
        *   Creates `AsyncClient` instance (using `httpx`).
        *   Returns `TelegramNotificationClient` instance.
    3.  Register as a singleton: `container.register(BaseNotificationClient, factory=init_telegram_notification_client, scope=Scope.singleton)`.
    4.  Add config fields to `settings/config.py`:
        *   `telegram_bot_token: str`
        *   `telegram_chat_id: str`
        *   `telegram_api_url: str = "https://api.telegram.org"`
    5.  Dependencies:
        *   Verify `httpx` is in `pyproject.toml` dependencies.
        *   Add environment variables to `.env.example`: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.

### Step 2: Extend ListenerAddedEventHandler to Notify Telegram ✅
*   **File:** `app/logic/events/messages.py` (lines 29-32)
*   **Current Code:**
    ```python
    @dataclass(frozen=True)
    class ListenerAddedEventHandler(EventHandler):
        async def handle(self, event: ListenerAddedEvent) -> None:
            # Outbox relay delivers the event to Kafka; no in-process side effect here.
            ...
    ```
*   **New Implementation:**
    ```python
    @dataclass(frozen=True)
    class ListenerAddedEventHandler(EventHandler):
        notification_client: BaseNotificationClient | None = None
        
        async def handle(self, event: ListenerAddedEvent) -> None:
            # Outbox relay delivers the event to Kafka
            # If Telegram client is configured, send notification
            if self.notification_client:
                notification = Notification(
                    title="New Listener Added",
                    text=f"Chat: {event.chat_oid}\nListener ID: {event.listener_oid}"
                )
                await self.notification_client.send(notification)
    ```
*   **Changes in `init.py` (lines 228-232):**
    ```python
    new_listener_added_handler = ListenerAddedEventHandler(
        message_broker=container.resolve(BaseMessageBroker),
        connection_manager=container.resolve(BaseConnectionManager),
        broker_topic=config.new_listener_added_topic,
        notification_client=container.resolve(BaseNotificationClient),  # NEW
    )
    ```

### Edge Cases
*   Make `notification_client` optional (default `None`) so the handler works when Telegram is not configured.
*   Wrap `send()` in `try-except` to log failures without breaking the event pipeline.

### Step 3: Add Lifespan Management for AsyncClient ✅
*   **File:** `app/application/api/lifespan.py`
*   **Actions:**
    1.  Add `init_http_client()` and `close_http_client()` functions.
    2.  Store `AsyncClient` in container as a singleton with lifespan management.
    3.  Update main lifespan context manager to start/close the client.
    *   *Alternative:* Use `AsyncClient` context manager directly in `TelegramNotificationClient.send()` (creates new client per request — simpler but less efficient).

### Step 4: Write Integration Test ✅
*   **File:** `app/test/application/api/test_telegram_integration.py` (new)
*   **Test Cases:**
    1.  `test_add_telegram_listener_stores_in_repo` — verify listener is added to chat entity.
    2.  `test_listener_added_event_sends_telegram_notification` — mock `AsyncClient.get()`, verify it's called with correct URL/params.
    3.  `test_telegram_notification_failure_does_not_break_pipeline` — mock `send()` to raise exception, verify event completes.
*   **Dependencies:**
    *   Mock `AsyncClient` via `pytest-httpx` or `respx`.
    *   Use `init_dummy_container()` and override `BaseNotificationClient` with a spy.

### Step 5: Update Documentation ☐ (partially — see Implementation Status note above)
*   **Files:**
    *   `README.md` — add Telegram configuration section.
    *   `docs/known-issues.md` — remove Issue #4 or mark as resolved.
    *   `docs/adr/NNNN-telegram-integration.md` — document design decisions (optional vs required, error handling strategy).

### Implementation Checklist
*   [x] Add Telegram config fields to `settings/config.py` (`telegram_bot_token`, `telegram_chat_id`, `telegram_api_url`).
*   [x] Add `httpx` dependency (already in `pyproject.toml`).
*   [x] Register `TelegramNotificationClient` in `init.py` (as `BaseNotificationClient`, config-gated singleton).
*   [x] Update `ListenerAddedEventHandler.handle()` to send notifications.
*   [x] Wire `notification_client` in `build_mediator` (resolves only when configured).
*   [x] Add error handling (`try-except + logging`).
*   [x] Write integration tests (3 test cases + 2 command-flow tests in `test_telegram_integration.py`).
*   [ ] Update `.env.example` with Telegram variables (`.env.example` does not exist yet).
*   [ ] Update `README.md` with Telegram setup instructions (config table lacks `TELEGRAM_*`).
*   [ ] Remove Issue #4 from `known-issues.md`.
*   [ ] Manual verification: send test request to `/chats/{chat_oid}/listeners` endpoint.

### Verification Commands
*   `cd app && poetry run pytest app/test/application/api/test_telegram_integration.py -v` ✅
*   `cd app && poetry run pytest` # full suite ✅ (137 passed, 2026-09-10)