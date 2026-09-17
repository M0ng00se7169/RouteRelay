# User Guide: How to check if your latest commit works as planned

> **Audience:** Anyone on the project, including juniors. No prior knowledge of
> Kafka, MongoDB, or Docker required to run the checks in this guide.
> **Goal:** After you (`git commit`) or someone else pushes a change, confirm it
> *actually does what it was supposed to do* before you trust it, merge it, or
> build on top of it.

This project is a chat backend (FastAPI + Kafka + MongoDB) written in a
**DDD + CQRS + event-driven** style. The good news: you do **not** need Mongo
or Kafka running to verify most changes. Tests run against **in-memory fake
repositories** (see `CLAUDE.md` → "Tests use in-memory repos" and
`app/test/fixtures.py`). That is the whole secret to making verification easy.

---

## 0. Mental model: "what does it mean for a commit to work?"

A commit "works as planned" when **all three** are true:

1. **It is correct** — the code does what the ticket/spec asked.
2. **It does not break anything else** — existing tests still pass.
3. **It is wired correctly** — new handlers/repos are registered in the DI
   container (`app/logic/init.py`) and the Mediator, so the running app can
   actually reach them. A handler that is not registered is a *silent* break
   (known issue #8).

Verification = running the test suite + static checks + (optionally) reading
the diff against the plan. We do this in stages, cheapest first.

---

## 1. Prerequisites (one-time setup)

Open a terminal in the project root (`E:\Studia\python\fastapi_examples`).

### 1.1 Install the toolchain

This project uses **Poetry** for dependency management. If you have never run
it:

```bash
poetry install
```

What this does: reads `pyproject.toml` and installs FastAPI, pytest, Motor,
aiokafka, ruff, pre-commit, etc. into a local virtual environment. Run it once
after cloning, and again whenever `pyproject.toml` changes.

> If `poetry` is not found, install it first: `pip install poetry` (or follow
> https://python-poetry.org/docs/#installation).

### 1.2 Confirm Python version

```bash
poetry run python --version
```

The repo targets a modern Python (3.12/3.13). If your system Python is old,
Poetry will tell you. Use `pyenv` or the project's pinned version.

### 1.3 (Optional) Make an editable shell

So you don't type `poetry run` every time:

```bash
poetry shell
```

Now `pytest`, `ruff`, etc. work directly. The rest of this guide assumes you
either prefix commands with `poetry run` or are inside `poetry shell`.

---

## 2. Step 1 — Make sure you are looking at the right commit

Before testing, confirm *what* you are testing.

```bash
git log --oneline -5
```

You should see your latest commit at the top. If you want to inspect the exact
changes in the most recent commit:

```bash
git show                 # full diff of the last commit
git show --stat          # just the list of changed files
```

If the commit is already pushed and you pulled it, `git log` still shows it.

> **Why this matters:** "the latest commit" is a moving target on a shared
> branch. Always eyeball `git log` first so you test the commit you *think* you
> are testing.

---

## 3. Step 2 — Run the test suite (the main event)

This is the single most important check. It needs **no Mongo, no Kafka**.

> **Critical path rule:** you must run pytest **from inside the `app/`
> directory**, not the repo root. The flat imports (`application`, `domain`,
> `infrastructure`, `logic`, `settings`, `test`) only resolve when `app/` is on
> the Python path (see `testing-rules.md` rule #1).

```bash
cd app
poetry run pytest
```

Expected output (abridged):

```
=================== test session starts ===================
...
collected 42 items

app/test/domain/values/test_messages.py ...           [  7%]
app/test/logic/test_messages.py ..                    [ 11%]
app/test/application/api/test_messages.py ...         [ 19%]
...
=================== 42 passed in 3.21s ====================
```

### 3.1 How to read the result

- **`42 passed`** → the commit does not break existing behavior. Good sign.
- **`X failed`** → something regressed. Read the failure block (see §7).
- **`X error`** → tests crashed during collection (often an import error, e.g.
  a missing DI registration). Also see §7.
- **A non-zero exit code** (`echo $?` shows `1` or more) → the check failed,
  even if the last line is ambiguous.

### 3.2 Run only the tests relevant to your change (faster feedback)

If the commit touches chats, you don't need to run everything:

```bash
# by path
poetry run pytest test/application/api/test_messages.py

# by keyword in the test name
poetry run pytest -k "create_chat"

# by marker
poetry run pytest -m asyncio
```

### 3.3 Run a single test with full traceback

```bash
poetry run pytest test/logic/test_messages.py::test_create_chat_command_success -vv
```

`-vv` = extra verbose (shows each assertion and fixture).

---

## 4. Step 3 — Run the linter and formatter (catches "wired but ugly" breakage)

Even if tests pass, the project enforces **Ruff** (lint + format) and
**pre-commit** hooks before merge. A commit that fails these will be rejected
by CI / pre-commit, so check locally:

```bash
poetry run ruff check .
poetry run ruff format --check .     # shows files that would be reformatted
```

If Ruff complains, auto-fix what it safely can:

```bash
poetry run ruff check . --fix
poetry run ruff format .
```

Project style is fixed and non-negotiable (from `CLAUDE.md`):
- line length **100**
- single quotes `'...'`
- **tabs** for indentation

So do not "fix" Ruff by changing the config; fix the code.

### 4.1 Pre-commit (the full gate)

```bash
poetry run pre-commit run --all-files
```

This runs *all* hooks (Ruff + isort + any others). Run this before you consider
the commit done. It is the same gate CI runs.

---

## 5. Step 4 — Verify the change matches the plan (the "as planned" part)

Tests prove the code *runs*; they don't prove it does *what the ticket asked*.
To close that gap:

### 5.1 Find the plan

Plans live as markdown under `.scratch/<feature>/` (see
`docs/agents/issue-tracker.md`):

- Spec: `.scratch/<feature>/spec.md`
- Tickets: `.scratch/<feature>/issues/NN-<slug>.md`

Read the ticket your commit implements.

### 5.2 Map the diff to the ticket

For each acceptance criterion in the ticket, confirm there is code + a test:

```bash
git show --stat          # which files changed
git show <file>          # see the actual change
```

A healthy feature commit typically touches **all** of these (per
`feature-workflow.md`):

| Layer | File(s) | What to look for |
|-------|---------|------------------|
| Command/Query/Event | `app/logic/commands/messages.py`, `app/logic/queries/messages.py`, `app/domain/events/messages.py` | frozen dataclass + handler, `_mediator.publish(...)` at end of command handler |
| DI wiring | `app/logic/init.py` | `container.register(<X>Handler)` **and** `mediator.register_command/query/event(...)` |
| API route + schema | `app/application/api/messages/` | route delegates to `mediator.handle_command/query`; response uses `from_entity` |
| Test | `app/test/...` | at least one happy-path test using the in-memory container |

> **Junior trap #1 — the silent DI miss:** if a handler is added but
> `container.register(<X>Handler)` is forgotten, tests that don't exercise it
> still pass, but the *running app* can't reach it (known issue #8). Always
> confirm the handler is registered in `init.py`. grep it:
> ```bash
> grep -n "MyNewHandler" app/logic/init.py
> ```

### 5.3 Confirm the test actually covers the behavior

A test named `test_create_chat_success` that only asserts `response.is_success`
is weak. Open the test file and check it asserts the *specific* outcome the
ticket wanted (e.g. the created title matches, or a duplicate raises
`ChatWithThatTitleAlreadyExistsException`, as in
`app/test/logic/test_messages.py`).

---

## 6. Step 5 — (Optional) Verify behavior live without infra

You can drive the real FastAPI app through its HTTP layer using the test
client, which swaps Mongo/Kafka for in-memory fakes automatically
(`app/test/application/api/conftest.py` overrides `init_container` with
`init_dummy_container`).

Example (run inside `app/`):

```python
# scratch_check.py  (delete after)
from application.api.main import create_app
from fastapi.testclient import TestClient
from test.fixtures import init_dummy_container
from logic.init import init_container

app = create_app()
app.dependency_overrides[init_container] = init_dummy_container
client = TestClient(app)

r = client.post(app.url_path_for('create_chat_handler'), json={'title': 'hello'})
print(r.status_code, r.json())
```

Run it: `poetry run python scratch_check.py`. This proves the *route → handler
→ repository* path end to end with zero external services.

> Do **not** add this as a permanent test file — it is just a quick manual probe.
> Real verification belongs in `app/test/`.

---

## 7. Step 6 — Understanding and acting on failures

### 7.1 A test FAILED (red F)

pytest prints the failing test name and the assertion that broke:

```
>       assert response.is_success
E       assert False
E       assert 500 == 200
```

How to act:
1. Note the test name and the line.
2. Run it alone with `-vv` (§3.3) to see exact values.
3. Reproduce by reading the handler the test exercises.
4. Fix the code (not the test) unless the test itself encodes a wrong
   assumption.
5. Re-run just that test, then the full suite.

### 7.2 A test ERRORED (collection error)

Often looks like:

```
ImportError: cannot import name 'X' from 'logic.commands.messages'
```

or a `punq` resolution error at startup. This usually means:
- a forgotten `container.register(...)` in `init.py` (§5.3), or
- a typo / renamed symbol the mediator wiring still references.

Fix the wiring, re-run.

### 7.3 Ruff failure

```
error: ... (E501) line too long
```

Fix the line (or run `ruff format .`). Don't lower the line-length budget.

---

## 8. Step 7 — (Heavy option) Full stack with Docker

Only do this if the commit touches **infrastructure wiring** (Kafka topics,
Mongo replica set, the outbox relay, observability) and you must see it live.
Otherwise skip — the in-memory suite is sufficient for feature correctness.

From the repo root:

```bash
make all          # Mongo (replica set) + Kafka + app + Grafana/Loki/Prometheus
```

Then:

- API docs: http://localhost:8000/api/docs
- Metrics: `GET http://localhost:8000/metrics`
- Mongo Express: http://localhost:28081
- Kafka UI: http://localhost:8090
- Prometheus: http://localhost:9090

When done:

```bash
make all-down
```

> **Known gotcha (from `CLAUDE.md`):** Mongo runs as a **replica set**
> (`--replSet rs0`). Writes use transactions, so `.env` must use
> `MONGO_DB_CONNECTION_URI=mongodb://mongodb:27017?replicaSet=rs0`. A standalone
> Mongo will make transaction-based writes fail.

---

## 9. Definition of "verified"

You can tell your reviewer / merge bot the commit is verified when **all** of
these are green:

- [ ] `cd app && poetry run pytest` → all passed
- [ ] `poetry run pre-commit run --all-files` → clean
- [ ] The diff in `git show --stat` maps to the ticket's acceptance criteria
- [ ] Every new handler is registered in `app/logic/init.py` (grep check)
- [ ] At least one in-memory test covers the happy path of the change
- [ ] (If infra-related) full-stack `make all` smoke test passed

If any box is unchecked, the commit is **not** verified yet — fix or ask.

---

## 10. Quick command cheat-sheet

```bash
# from repo root
poetry install                      # one-time / after deps change
poetry shell                        # optional: enter venv

# verify (the loop you'll repeat)
cd app
poetry run pytest                  # main correctness check (no infra needed)
poetry run pytest -k "create_chat" # narrow run
poetry run ruff check .            # lint
poetry run pre-commit run --all-files  # full gate

# inspect
git log --oneline -5
git show --stat
grep -n "MyHandler" app/logic/init.py

# heavy / live (optional)
make all        # from repo root
make all-down
```

---

## 11. FAQ for juniors

**Q: Do I need MongoDB/Kafka installed to verify a normal commit?**
A: No. `pytest` uses in-memory fakes via `init_dummy_container`
(`app/test/fixtures.py`). Only the `make all` Docker path needs real services.

**Q: Why must I `cd app` before pytest?**
A: The code uses flat imports (`application`, `domain`, ...). Those only resolve
when `app/` is on `sys.path`, which the test layout guarantees from inside
`app/`. Running from the repo root gives `ModuleNotFoundError`.

**Q: Tests pass but the app "does nothing" at runtime — why?**
A: Almost always a missing `container.register(<X>Handler)` in
`app/logic/init.py` (known issue #8). Tests that don't hit that handler stay
green; production can't reach it. Always grep `init.py`.

**Q: `pytest` hangs or times out on a message endpoint.**
A: `init_dummy_container` overrides `BaseChatsRepository` but **not**
`BaseMessagesRepository` (known issue #7). A message test needs a memory
messages repo registered, or it falls through to real Mongo and times out. See
`testing-rules.md` rule #3.

**Q: Ruff wants tabs and single quotes — do I edit config?**
A: No. Edit the code to match the project style (from `CLAUDE.md`).

**Q: How do I know what the commit was *supposed* to do?**
A: Read the ticket/spec under `.scratch/<feature>/` (see
`docs/agents/issue-tracker.md`). "Works as planned" means the diff satisfies
that spec.
