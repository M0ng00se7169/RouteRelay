# Flow Control Extension

`~/.pi/agent/extensions/flow-control.ts` — главный контроллер стадий флоу (grill → spec → tickets) в голом TUI pi и в headless.

## UX-контракт (после фикса 2026-09-24, усилен guard-ом интервью)

**Delivery только в конце интервью (фикс 2026-09-24, вторая итерация):**

- Guard в `flow-control.ts` блокирует `json_output_grill`, пока не выполнены ОБА условия:
  ≥ 2 ответа пользователя И ≥ 2 заданных вопроса (assistant-ходы, оканчивающиеся на `?`).
- Счётчик вопросов живёт в двух слоях: live (`message_end`) + история сессии (`sessionManager.getEntries()`),
  как и `effectiveUserTurns` — resume не теряет прогресс интервью.
- Раннее отклонение говорит модели: «interview is NOT complete → задай следующий вопрос текстом».
- После 2 отказов guard сдаётся (accept + предупреждение) — модель не зациклится, артефакт не потеряется.
- `grill.schema.json`: `status` теперь только `grill_complete` (убран `need_info` — модель трактовала его
  как приглашение к промежуточной доставке); в описании поля сказано: неотвеченное → `open_questions`.
- `grill.md`/`SKILL.md`: явное правило SINGLE FINAL DELIVERY — «call exactly once, only after ALL
  questions (2–5) are asked and answered».

**Run id больше не обязателен.** `/grill <task>` можно вызывать без `/flow`:

- При доставке валидного JSON (тулом или гейтом) без заданного run назначается **авто run id** `run-YYYYMMDD-HHMMSS`, артефакт пишется в `.agent/runs/<run_id>/<stage>.json`.
- Авто id наследуется следующими стадиями (`currentRun`), так что `/to-spec` без аргументов подхватит тот же run.
- Промпт-тул в ответе сообщает: `Written to ...` + подсказка про именованные run при авто-назначении.
- Раньше без `/flow` артефакт оставался «chat-only» («No run set — not saved to disk») — это и был UX-баг.

**Промпты больше не едят первый аргумент как run_id:**

- `.pi/prompts/grill.md`: `argument-hint: "<task description>"`, текст задачи = `$ARGUMENTS` (pi подставляет ВСЕ аргументы, а не `$1`). Промпт `/grill I need a very basic authentication to the app` больше не создаёт run id "I".
- `.pi/prompts/to-spec.md`, `.pi/prompts/to-tickets.md`: `argument-hint: "[run_id]"`, дефолт `${1:-auto}`; при `auto`/отсутствии каталога — взять НОВЕЙШИЙ каталог в `.agent/runs/` с нужным артефактом.

**json-output.ts удалён из расширений** (переехал в `~/.pi/agent/extensions/backup/json-output.ts.bak`): инертен без `--json-schema`, дублировал promptGuidelines. Единственный активный контроллер — flow-control.ts.

## Гигиена контекста (фикс 2026-09-24, третья итерация)

- При активации `spec`/`tickets` extension проверяет, есть ли в текущей сессии user-ходы
  (live-счётчик + `sessionManager.getEntries()`). Если есть — warning:
  «session already has N user turn(s) from a previous stage — start a FRESH session».
- Правило: **одна стадия = одна сессия**. Grill живёт в своей сессии (`-c` между ходами интервью),
  spec/tickets стартуют с чистым контекстом и читают артефакт из `.agent/runs/<run_id>/`.
- Headless-нюанс: `--no-session` + `-p` — сессия эфемерна, warning не срабатывает (и не нужен).
- Headless-нюанс №2: `ui.notify` НЕ печатается в `--mode text/json` (тихо дропается) — виден
  только в TUI или `--mode rpc` (`extension_ui_request`). Проверять гварды в headless — через rpc.

## Команды

- `/flow grill|spec|tickets [run_id]` — активировать стадию; per-stage тула `json_output_<stage>` со схемой в сигнатуре (Type.Unsafe), ajv-валидация, terminate:true, лимиты тулов (grill: только json-тул; spec/tickets: +read/write/bash).
- `/flow off` — восстановить все тула (с проверкой недоставленного артефакта).
- `/flow` — статус (run, stage, delivered/pending).
- Headless: `--flow "grill:run_id"` + `--flow-fail-hard` (exit 1 без валидного JSON).

## Защита от циклов

- `mkdirSync(dirname(out), {recursive:true})` перед writeFileSync (ENOENT на новом каталоге run вызывал вечный ретрай).
- Лимит: 3 отклонённые валидацией попытки → тул сдаётся и велит модели сообщить об ошибке.
- Ошибка записи тоже капится и не предлагается к слепому ретраю.

## Связка с models-store

- `supportsStrictMode: true` в каноне (`~/.pi/agent/models.json`) = серверный grammar-форс схем тулов на llama-server — первый слой контракта.
- `store-guard.ts` чинит дрейф `models-store.json` по канону на каждом session_start — второй слой.
- `samplingParams.response_format` в models.json **не используется сознательно** (проверено живыми тестами 2026-09-24): режим глобален на модель, `json_object` превращает текстовые ходы интервью в JSON-мусор, `json_schema` вытесняет tool_call. Тюнинг сэмплинга — в config.ini роутера.
