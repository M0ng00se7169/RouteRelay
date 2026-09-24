---
name: grill-with-docs
description: Interview about a plan and record decisions. Use at the start of a new feature or task to clarify requirements, constraints, and non-goals before writing any spec or code.
---

# Grill With Docs

Get interviewed about a plan, and record the decisions.

## When to Use

- Starting a new feature or task
- Requirements are unclear or incomplete
- Need to record decisions before implementation

## Process

1. Read the task idea or raw requirements provided by the user.
2. **Do NOT read project files.** Do not call `read`, `grep`, `find`, or `ls`. Interview the user from the information in the conversation and the task text alone. If a fact about the codebase would change the design, ask the user about it as a question — the user knows the codebase.
3. Ask at least 2 and at most 5 focused questions, one at a time, waiting for each answer. Only ask blocking questions if the answer materially changes implementation.
4. Record all decisions, constraints, non-goals, and assumptions.
5. If critical information is missing, list blocking open questions.
6. Finish by calling the json_output tool ONCE — a SINGLE FINAL delivery after the interview is complete (all questions asked and answered). Never deliver mid-interview: a guard counts asked questions and rejects premature deliveries. Do not print the JSON in chat.

Context budget: this stage must fit in ~4k tokens of conversation. Reading even one source file can consume 2-5k tokens and starve the interview. Do not read files, period.

## Output Format

Write a JSON file with this structure as schema [Output Schema](../../schemas/grill.schema.json)

## Rules

- Do NOT write code.
- Do NOT invent requirements.
- Do NOT read any files or run exploration commands — zero `read`/`grep`/`find`/`ls` calls. This is a hard rule, not a preference.
- Do NOT print the JSON artifact in chat — deliver it via the json_output tool.
- NEVER deliver in your first reply: the first reply is always a question to the user. A guard rejects instant delivery.
- NEVER deliver mid-interview: call json_output exactly once, after ALL your questions (2-5) are asked and answered. Premature delivery is rejected; if rejected, continue asking questions in plain text.
- Keep questions focused and actionable.

## Examples

You can find examples of grilling in the [examples directory](./references/examples.md).