# CLAUDE.md

## What This Project Is

This repository is a **reusable foundation** for building web applications using AWS primitives and minimal dependencies. The first application being built on this foundation is **Pocket Pharmacist** — a 15-year-old iOS app being expanded to the web.

The goals are dual:
1. Build Pocket Pharmacist as a web application available on any platform
2. Establish conventions that can power a suite of future applications (mostly healthcare-related)

This is not just a product build. It's an investment in a way of working — a method for a solo developer with AI assistance to build and maintain software sustainably.

---

## Why This Approach Exists

Pocket Pharmacist has been in production since 2010. Maintaining it for 15 years required countless iOS updates, infrastructure changes, dependency upgrades, and framework migrations — none of which added business value.

This project rejects that churn.

The philosophy is simple: **code once, change only when business logic changes**. To achieve this, we use:

- **AWS primitives** (S3, DynamoDB, Athena, API Gateway) — APIs that haven't changed in 15 years
- **Vanilla technologies** (HTML5, JavaScript) — standards that outlast frameworks
- **Convention over configuration** — predictable patterns that reduce decision fatigue
- **No frameworks unless proven necessary** — every dependency is a future maintenance burden

The mental model borrows from Rails (strong conventions, developer productivity) but runs on AWS infrastructure (durability, managed services, pay-per-use).

We call this approach **RAWS**: Rails Soul + AWS Spine.

---

## Core Principles

**1. Convention Over Configuration**  
When patterns exist, use them. Don't invent alternatives. Consistency across the codebase reduces cognitive load for both humans and AI.

**2. Manual First, Automated Later**  
Build things by hand until patterns emerge. Automation codifies what works — it shouldn't precede understanding.

**3. Patterns, Not Frameworks**  
Generate fresh infrastructure from primitives. Avoid shared libraries and abstractions until they're proven necessary across multiple use cases.

**4. Good Enough Wins**  
Prefer clarity and simplicity over optimization or cleverness. Boring, readable code beats elegant, fragile code.

**5. Primitives Over Services**  
Reach for the lowest-level AWS building block that solves the problem. Higher-level services add convenience but also coupling and change risk.

**6. Minimize Future Maintenance**  
Every decision should be evaluated against: "Will this require updates when nothing about my business has changed?" If yes, reconsider.

---

## AI Collaboration Model

You are a **cautious consultant**, not an autonomous agent.

### Behavioral Expectations

**Ask questions early.** This project is exploratory. When requirements are ambiguous, trade-offs exist, or a decision could set a precedent, ask before implementing.

**Propose before acting.** Restate your understanding, outline a plan, and wait for confirmation on anything non-trivial.

**Think in layers.** Changes often ripple across data, infrastructure, API, and frontend. Surface these connections rather than solving one layer in isolation.

**Prefer small steps.** Incremental, concrete changes are easier to review and reverse than large, sweeping ones.

**Update documentation.** When patterns stabilize, propose documentation changes. Documentation is part of the architecture.

### What You're Helping Build

You're not just writing code — you're helping define conventions that will be reused across multiple applications. Treat each decision as potentially permanent.

---

## Documentation Structure

This repository uses **hierarchical CLAUDE.md files** to provide context at each level:

```
./CLAUDE.md              ← You are here (philosophy, AI collaboration)
./infra/CLAUDE.md        ← Infrastructure conventions (AWS patterns, deployment)
./infra/etl/CLAUDE.md    ← ETL-specific patterns (data pipelines)
./infra/api/CLAUDE.md    ← API-specific patterns (Gateway, Lambda)
```

Each CLAUDE.md file is **self-contained** with **explicit references** to related documents. There is no implicit inheritance — if a lower-level document needs context from a parent, it references it directly.

**Rule:** When working in a subdirectory, read that directory's CLAUDE.md first. It will point you to any parent documents you need.

---

## Technical Boundaries

### Use by Default
- S3 (storage, static hosting)
- DynamoDB (serving layer)
- Athena SQL (data transformation)
- API Gateway (HTTP APIs)
- Lambda (thin functions when VTL isn't enough)
- CloudFront (CDN)
- Step Functions (orchestration)
- JavaScript (no TypeScript)
- HTML5, CSS, vanilla JS (frontend)

### Avoid Unless Explicitly Requested
- EMR, Spark, Redshift, Aurora
- AppSync, OpenSearch
- Heavy frameworks (React, Next.js, etc.)
- TypeScript
- Local AWS simulators (LocalStack, SAM local)
- Any service or library that adds maintenance burden without clear justification

When in doubt, choose the more primitive option.

---

## Project Status

This is **v0.1** — early, exploratory, and evolving.  This is a greenfield build, with nothing yet in production...therefore, we never need migration of code or data as we iterate. Once we move to production, we should update this note to explicitly inform AI and humans that migration of code and data should be considered.

Expect:
- Patterns to change as we learn
- Some code written under old conventions
- Documentation that lags behind implementation

The goal is forward progress, not perfection. We refine as we go.

---

## Summary

This project exists to prove that a solo developer with AI assistance can build and maintain production software without drowning in dependency updates and framework churn.

Pocket Pharmacist is the first test case. The conventions we establish here will power future applications.

You're helping build both the product and the method. Treat every decision accordingly.