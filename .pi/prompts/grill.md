---
description: Grill interview stage — asks questions, emits grill.json via json_output_grill
argument-hint: "<task description>"
---
Task: ${ARGUMENTS:-<the user will describe the task in their next message>}

If the task text above is a placeholder or empty, ask the user what to build BEFORE the interview.

Follow the skill .pi/skills/grill-with-docs/SKILL.md for interview rules.

Interview rules:
- FIRST REPLY = FIRST QUESTION. Your very first action is asking the user ONE focused question in plain text. NEVER call json_output_grill in your first reply — a guard blocks instant delivery.
- Ask at least 2 questions (up to 5), ONE at a time, wait for the answer each time.
- SINGLE FINAL DELIVERY: call json_output_grill EXACTLY ONCE, only AFTER the interview is fully finished (all 2-5 questions asked and answered). A mid-interview delivery is rejected by a guard — after each rejection, just ask your NEXT question in plain text.
- If the user says "done"/"достаточно" or no blocking unknowns remain, ask nothing more: deliver immediately.
- ZERO file/tool exploration: no `read`, no `grep`, no `find`, no `ls`. The conversation and task text are your ONLY inputs. If a codebase fact would change the design, ask the user about it instead of looking it up.
- Hard stop: after 5 questions, OR when the user says "done"/"достаточно", OR when no blocking unknowns remain.
- Anything unanswered and non-blocking goes to open_questions — do NOT keep asking.
- Do not re-ask what the user already answered.
- Context budget: keep the whole exchange small; every file you read risks blowing the 32k window and killing the interview.

Output:
- When the interview is complete, call the json_output_grill tool ONCE as your LAST action with the final grill JSON (the flow-control extension validates it against the schema and saves the artifact to disk — no /flow command is needed).
- Do NOT write files yourself. Do NOT print the JSON in chat.
