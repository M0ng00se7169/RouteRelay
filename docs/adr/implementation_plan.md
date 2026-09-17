# ADR: Comprehensive Implementation Plan

**Status:** Implemented ✅ (verified 2026-09-10)
**Date:** 2024-05-22
**Author:** AI Assistant
**Subject:** Vertical, Core-to-Bottom Implementation Strategy for Priority 1 Fixes

> **✅ Implementation Status (2026-09-10):** ALL CHUNKS DONE. Memory repo implements
> the full 7-method `BaseChatsRepository` contract, all command handlers are
> top-level container registrations, the Kafka consumer loop runs in the lifespan,
> and the test fixtures override both repositories (`MemoryChatRepository` +
> `MemoryMessagesRepository`) so the suite never touches real Mongo/Kafka.
> Verified by `cd app && poetry run pytest` — 137 passed.

## 1. Context

This ADR outlines the strategy for implementing the critical fixes identified in Priority 1 of the development roadmap. The goal is to address foundational architectural gaps (type safety, dead code paths, and repository completeness) by adopting a **Vertical, Core-to-Bottom** implementation strategy. This approach ensures that dependencies are established and validated from the most abstract domain layer down to the concrete API endpoints, minimizing the risk of downstream integration failures.

## 2. Decision: Vertical Implementation Strategy

We will proceed in four distinct, vertically-sliced chunks. Each chunk must be completed and verified before proceeding to the next.

**Rationale:**
*   **Core-to-Bottom:** By starting with the Domain and Infrastructure layer, we establish the contracts (Entities, Repositories) first. This forces us to define the data structures and interfaces before the business logic tries to consume them, ensuring type safety (Priority 1 fix #1).
*   **Vertical Slicing:** Each chunk will focus on a single responsibility slice (e.g., all Repository interfaces, then all Command handlers). This allows for isolated testing and verification of contracts before they are integrated into the larger system.

## 3. Implementation Plan (Chunks)

### Chunk 1: Core Domain & Infrastructure Foundation (The Contract Layer) ✅

**Focus:** Establishing the data contracts and communication backbone.
**Goal:** Complete the Repository interfaces and fix immediate type safety issues.
**Order:** Domain Entities $\rightarrow$ Base Repository Interfaces $\rightarrow$ Kafka Broker Setup.

**Tasks:**
1.  **Domain Layer Finalization:** Review and finalize all `domain/entities/` to ensure they are clean and immutable.
2.  **Repository Interface Completion:** Implement the missing 4 methods in `app/infrastructure/repositories/messages/memory.py` to satisfy the full `BaseChatsRepository` contract.
3.  **Type Safety Fix:** Apply the annotation fix in `app/logic/events/messages.py` (Priority 1, Issue #1).
4.  **Kafka Broker Setup:** Ensure the Kafka consumer loop is started in `app/application/api/lifespan.py` (Priority 1, Issue #5).

**Verification:** Run `poetry run pytest` to ensure all repository methods pass tests and type checkers (like Mypy) pass without errors.

---

### Chunk 2: Business Logic & Mediator (The Orchestration Layer) ✅

**Focus:** Implementing the command and query handlers that orchestrate the flow.
**Goal:** Ensure all command handlers are correctly registered in the DI container.
**Order:** Command/Query Handlers $\rightarrow$ Mediator Setup.

**Tasks:**
1.  **Handler Registration Fix:** Modify `app/logic/init.py` to ensure **all** command handlers (`CreateChatCommandHandler`, `CreateMessageCommandHandler`, etc.) are registered as top-level container registrations, resolving Priority 1, Issue #8.
2.  **Mediator Finalization:** Verify the `app/logic/mediator/base.py` is correctly dispatching events and that the flow from Command $\rightarrow$ Mediator $\rightarrow$ Event Handler is sound.

**Verification:** Run `poetry run pytest` specifically targeting the command handlers to ensure they can be resolved and executed correctly via the container.

---

### Chunk 3: API & Presentation Layer (The Edge Layer) ✅

**Focus:** Wiring up the external interfaces (FastAPI, WebSocket).
**Goal:** Connect the business logic to the external world and ensure the application lifecycle is correct.
**Order:** API Routers $\rightarrow$ Lifespan Configuration.

**Tasks:**
1.  **API Routing:** Ensure all FastAPI routes in `application/api/` are correctly wired to the Command/Query handlers in `app/logic/`.
2.  **Lifespan Configuration:** Confirm the application lifespan correctly initializes all necessary services (Kafka, Repositories) before the application starts serving requests.

**Verification:** Run `poetry run uvicorn --factory application.api.main:create_app --reload` to ensure the application starts successfully and the API endpoints are reachable.

---

### Chunk 4: Testing & Integration (The Validation Layer) ✅

**Focus:** Validating the entire vertical stack.
**Goal:** Ensure the system is robust and testable.
**Order:** Test Fixtures $\rightarrow$ Full Integration Tests.

**Tasks:**
1.  **Test Fixture Update:** Update `app/test/fixtures.py` to correctly configure the in-memory repositories and override the necessary base repositories for message tests (Priority 1, Issue #7).
2.  **End-to-End Flow Test:** Write a new integration test that simulates a full message creation flow, verifying that the Kafka event is published and the WebSocket client receives the update.

**Verification:** Run `cd app && poetry run pytest` to confirm all tests pass, covering domain logic, repository contracts, and the end-to-end flow.

## 4. Next Steps

~~Begin execution by starting **Chunk 1**. Once verified, proceed to Chunk 2.~~
All chunks are implemented and verified (see Implementation Status at the top).
