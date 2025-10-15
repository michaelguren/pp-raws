# RAWS: Rails Soul, AWS Spine

### Pocket Pharmacist – Architecture Strategy (v0.2 Manifesto)

---

## 1. Introduction: The RAWS Ethos

**RAWS** is our answer to the modern web problem: too many moving parts, too little coherence.  
Rails gave us joy, but it forgot about deployment, scaling, and the cloud.  
AWS gives us power, but it forgot about humans.

RAWS brings them together — the **soul of Rails** with the **spine of AWS** — uniting app code, infrastructure, data, and AI collaboration under a single set of conventions.

This is not a framework you install. It’s a _philosophy you evolve._  
We build it by hand first, so AI can learn what works and codify it later.

RAWS’ goal is simple:

> Enable one developer to build, deploy, and evolve modern AWS-native applications with the same joy, clarity, and speed that Rails brought to the early web.

---

## 2. Core Principles

### 2.1 Convention Over Configuration

There are a million ways to solve every AWS problem. We will pick one.  
Opinionated defaults, clear folder structures, predictable naming, and repeatable patterns — that’s the RAWS way.

### 2.2 Manual Before Automated

Every generator starts as a hand-built example. We earn automation by understanding what deserves to exist.  
We don’t pave cow paths until we’ve walked them — slowly, with intention.

### 2.3 Patterns, Not Libraries

RAWS Core is not a library.  
It’s a living catalog of **reproducible patterns** — minimal, explicit examples that express architectural intent.  
AI and developers use these patterns as source material to generate infrastructure directly from AWS primitives each time, ensuring every stack is born clean, current, and context-specific.  
There are no dependencies — only conventions.

### 2.4 Good Enough > Perfect

Perfect systems rot.  
“Good enough” systems evolve.  
Every choice — schema, service, search — should maximize clarity, not complexity.

### 2.5 Infrastructure as Convention

Infra lives with code, versioned, testable, and explainable.  
AWS CDK is our compiler. Each RAWS domain maps predictably to infrastructure resources and API routes.

### 2.6 AI-Augmented Development

AI isn’t our intern; it’s our collaborator.  
We document patterns through `CLAUDE.md` and `README.md` files so that humans and machines share the same source of truth.  
Over time, AI becomes the builder of builders — codifying what we discover.

---

## 3. The Medallion Architecture (Bronze → Silver → Gold)

We preserve the **Medallion pattern** because it works — but we make it our own.

| Layer      | Purpose                                                                          | Example                                         |
| ---------- | -------------------------------------------------------------------------------- | ----------------------------------------------- |
| **Bronze** | Ingest raw external data exactly as received. Immutable, auditable, versioned.   | FDA NSDE, RxClass, RxNorm downloads             |
| **Silver** | Refine and normalize data. Apply structure, quality checks, and transformations. | Flatten NDC lists, standardize column names     |
| **Gold**   | Curated, consumable data. Ready for app-level access, APIs, and analytics.       | `gold.drug_reference` for Pocket Pharmacist app |

**Folder Convention**: In RAWS ETL projects, datasets are organized by medallion layer:
```
datasets/
├── bronze/{dataset}/    # Raw ingestion datasets
├── silver/{dataset}/    # Transformed/joined datasets
└── gold/{dataset}/      # Curated, analytics-ready datasets
```

This structure ensures consistency across all RAWS projects and makes the data lineage immediately clear.

Each layer is **idempotent** and **disposable**.  
Glue Jobs perform the work; Step Functions orchestrate flow.  
S3 and Athena form the lakehouse — the simplest expression of truth.  
No warehouses, no clusters, no waiting rooms for your data.

RAWS also embraces **adaptive complexity**.  
Layers exist to clarify intent, not to create ceremony.  
When a dataset requires no transformation beyond ingestion, Bronze and Silver may safely collapse.  
RAWS evolves by need, not by ritual.

---

## 4. Operational Data Layer (DynamoDB)

The **Gold layer** becomes the source of truth for the operational world.

RAWS treats DynamoDB as the **operational twin** of the warehouse — designed for API-speed access, not analytics.

We divide responsibilities clearly:

| Task                                             | Tool                         | Principle                              |
| ------------------------------------------------ | ---------------------------- | -------------------------------------- |
| **Transport** (move, store, retrieve)            | VTL (API Gateway ↔ DynamoDB) | Zero-ops, deterministic                |
| **Transform** (logic, side effects, aggregation) | Lambda                       | Code only where expression requires it |

We don’t yet know our final PK/SK standard — and that’s intentional.  
RAWS v0.2 is about _learning the right shape of data._  
The structure must balance readability, scalability, and AI composability.

---

## 5. “Good Enough” Full-Text Search

Let’s be clear:  
RAWS is **grossly unimpressed** with current search stacks.

OpenSearch, Elasticache, and their enterprise cousins are beautiful sledgehammers built to crush the tiniest nails.  
They are expensive, complex, and over-engineered for 99% of what modern applications need.

RAWS takes a simpler stance: **search should be local, deterministic, and cheap.**

### The Principle

If the data already lives in DynamoDB, we should be able to find it in DynamoDB.

### The Approach

We’ll tokenize searchable fields into prefix-addressable items.  
Each token becomes a lightweight index record (`SEARCH#DOMAIN#FIELD`, `token#prefix#id`).  
Queries use `begins_with` conditions, not scans.  
It’s “good enough” — and that’s exactly the point.

### The Philosophy

- No extra clusters
- No external sync pipelines
- No relevance scoring fairytales
- Just **fast**, **predictable**, and **explainable** search.

RAWS prefers a well-balanced screwdriver over a fleet of pneumatic drills.

---

## 6. Application Layer (API + Frontend)

Every layer speaks the same language: convention.

### API Layer

- API Gateway + VTL for CRUD (no Lambda unless transforming).
- Routes are declarative, domain-scoped, and registered by convention.
- Auth flows (Cognito or future replacements) integrate directly via JWT.

### Frontend

- Static web hosting via S3 + CloudFront.
- Minimal stack: vanilla JS, HTMX, or any future framework adhering to simplicity.
- The app reads the same route manifest the backend generates.

The frontend and backend are separate projects, but **one mental model**.  
Everything is deterministic and inspectable.

---

## 7. AI as Compiler

RAWS evolves through conversation between humans and machines.

- **Humans** craft the first working patterns.
- **AI** observes, generalizes, and codifies them into generators.
- Together, we create a _living framework_ that improves with each iteration.

But AI is not a junior developer — it’s the **compiler of intent**.  
RAWS moves toward **spec-driven development**, where humans express what they need, and AI generates the primitives that satisfy those specifications.  
The manifests, domain files, and configs become the source language; AWS becomes the runtime.  
We don’t code infrastructure — we _declare intent_ and let AI compile it.

The `CLAUDE.md` and `README.md` files are the connective tissue between intent and implementation — the grammar of our meta-architecture.

---

## 8. Evolution Path (v0.2 → v1.0)

RAWS v0.2 is about **manual validation**, not production polish.

| Stage     | Goal                                                                                                             |
| --------- | ---------------------------------------------------------------------------------------------------------------- |
| **v0.2**  | Prove reproducible patterns: manual CRUD + ETL end-to-end.                                                       |
| **v0.3**  | Automate stable patterns (route generation, VTL scaffolds).                                                      |
| **v0.4+** | Extend to multi-app deployments (ETL + operational API + static web).                                            |
| **v1.0**  | A full-stack, spec-driven, convention-based AWS meta-architecture: one command, one deploy, one source of truth. |

Every iteration teaches us what deserves to be automated.  
RAWS evolves at the speed of confidence.

---

## 9. The RAWS Manifesto

> **We reject accidental complexity.**  
> **We reject “enterprise” as an excuse for opacity.**  
> **We reject the idea that modern apps must be expensive to run.**

We believe:

- A single developer should deploy a full AWS-native stack with clarity and joy.
- Infrastructure should feel as natural as defining a model.
- ETL, CRUD, and Search belong to the same ecosystem, not different worlds.
- “Good enough” is often perfect.
- AI is the compiler, not the coder.

RAWS is not a product. It’s a way forward — for developers, for AI, and for the next generation of simple, powerful software.

---

_Version: RAWS v0.2 Manifesto — October 2025_
