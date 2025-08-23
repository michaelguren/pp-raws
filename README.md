# RAWS: Rails Soul, AWS Spine - v0.1 Implementation Plan

## Overview

RAWS (Rails + AWS) is a convention-based serverless framework designed to deliver a Rails-like developer experience while fully embracing AWS's infrastructure. Inspired by Ruby on Rails' philosophy of convention-over-configuration and rapid development, RAWS addresses a key shortcoming of Rails: the lack of integrated hosting and deployment strategies. By tightly coupling application code with AWS-native services through Infrastructure as Code (IaC), RAWS enables developers to go from idea to production seamlessly, without managing servers or worrying about scaling pitfalls. It's targeted at solo developers and small teams building scalable web applications, APIs, and data-driven services for under $10/month. Built for fast iteration without sacrificing long-term maintainability, it's single-developer focused—pure JavaScript, no TypeScript ceremony.

The core idea is to abstract AWS complexities behind Rails-style generators and commands, while leveraging serverless primitives for zero-ops scaling. For v0.1, we prioritize manual validation before automation: hand-written VTL templates, a "magic helper" for route registration, and basic testing—before adding generators or complexity. This embodies textbook Rails philosophy: start with the simplest thing that could possibly work, make it work, then iterate to build confidence and create working examples for future enhancements.

# REPOMIX

```bash
repomix . \
 --style markdown \
 --compress \
 --output-show-line-numbers \
 --header-text "Forget all previous project files. Only use the file I'm uploading now as the sole source of truth." \
 -o ~/Desktop/raws.txt
```

## Why RAWS?

- **Unified Code and Infrastructure**: Rails excels at MVC but leaves deployment as an afterthought (e.g., Heroku, custom EC2). RAWS solves this by generating AWS CDK constructs alongside code, ensuring architecture and hosting evolve together.
- **Serverless by Default**: Eliminate cold starts and overhead with direct integrations (e.g., API Gateway to DynamoDB via VTL), keeping costs minimal and performance high.
- **"Good Enough" Philosophy**: Prioritize simplicity and evolvability—start with MVPs that scale to hundreds of thousands of users, then enhance without refactoring.
- **Developer Joy**: Rails-like CLI for scaffolding, with auto-generated VTL templates, API routes, and DynamoDB schemas.

## Core Features

- **No-Lambda CRUD Operations**: Direct API Gateway integrations to DynamoDB using Velocity Template Language (VTL) for request/response mapping, avoiding function overhead.
- **Full-Text Search**: Built-in inverted indexes on DynamoDB partition keys/GSIs for "good enough" search without Elasticsearch.
- **Single-Table DynamoDB Design**: Optimized for efficiency, with automated schema generation and query patterns.
- **Authentication**: Cognito for user pools, JWT, and role-based access, wired into API routes.
- **Data APIs**: RESTful endpoints auto-generated for domains (e.g., GET/POST /users).
- **ETL Pipelines**: Serverless data ingestion/transformation using Glue and Step Functions.
- **Static Hosting & CDN**: S3 + CloudFront for frontends, with optional Amplify integration.
- **Progressive Scaling**: Escape hatches for adding Lambda, Kinesis, or other services as needs grow.

## Technical Strategy

RAWS uses AWS CDK for IaC, ensuring one-command deploys. The framework generates code and infrastructure based on domain definitions (e.g., `raws generate domain users --fields name:string email:string --searchable name`). For v0.1, we prioritize manual validation before automation.

1. **Domain-Driven Generation**:

   - Define entities (domains) with schemas, operations (CRUD), and searchable fields.
   - Auto-generates: DynamoDB table schemas (single-table with composite keys), GSIs for search, and query patterns.
   - **v0.1 Approach**: Start with manual domain setup to prove concepts, then build generators targeting these examples.

2. **VTL Generators**:

   - For each domain operation (e.g., CREATE_USER), generate VTL templates for API Gateway integrations.
   - Templates handle validation, DynamoDB operations (PutItem/GetItem), and response formatting.
   - Example: `USER_CREATE.request.vtl` builds PutItem with input validation; supports custom logic via VTL scripts.
   - **v0.1 Approach**: Begin with hand-written VTL templates to learn quirks (e.g., error handling, auth integration). This creates concrete examples for generators to emulate later.

3. **API Route Generators (for CDK)**:

   - Creates API Gateway REST APIs with routes (e.g., POST /users, GET /users/{id}).
   - Attaches VTL integrations, Cognito authorizers, and request/response models.
   - CDK constructs auto-wire routes to DynamoDB, with caching via CloudFront.
   - **v0.1 Approach**: Emphasize the "magic helper" `registerCrudRoutes` function, which auto-wires routes to VTL templates and adds Cognito auth. Focus on proving this core framework piece works for basic CRUD.

4. **Infrastructure as Code**:

   - CDK stacks for all resources: DynamoDB tables, API Gateway, Cognito, Glue jobs, etc.
   - Modular: Core stack for shared infra, domain-specific stacks for extensions.

5. **Data Warehouse ETL**:

   - A lightweight, serverless data lake built on S3 for storage, AWS Glue for ETL jobs, and Athena for SQL queries.
   - RAWS scaffolds this with CDK, creating partitioned S3 buckets (raw/curated zones) where data is versioned via paths (e.g., `/curated/dataset/version=2025-08-05/`).
   - EventBridge schedules Glue jobs (PySpark scripts) at set times (e.g., 3am) to ingest external data (e.g., FDA NSDE CSV and NDC zip files), flatten/transform it (e.g., add SCD2 versioning, format NDCs, update repackager flags), and write Parquet files.
   - Glue Crawlers auto-catalog schemas for Athena, enabling point-in-time queries like `SELECT * FROM table WHERE version <= '2025-08-05'`.
   - For app integration, expose via AppSync GraphQL or API Gateway + Lambda proxies; load transformed data into DynamoDB for operational queries.
   - Start simple with one job per dataset; enhance with Step Functions for multi-step orchestration, including escape hatches for streaming (e.g., Kinesis Firehose).
   - **Key Benefits**: Pay-per-use (Glue ~$0.44/hour, Athena ~$5/TB scanned), no idle costs, and native AWS integration for full-stack analytics. Handles batch or near-real-time via Kinesis if needed.

6. **Security & Observability**:
   - Least-privilege IAM roles auto-generated.
   - CloudWatch/X-Ray for monitoring; built-in logging in VTL/Glue.

## Developer Experience

- **CLI Commands**:

  - `raws new app-name`: Scaffold project with CDK app, domains folder, and config.
  - `raws generate domain <name> --fields <fields> --searchable <fields> --operations crud`: Creates schema, VTL templates, API routes, and CDK resources.
  - `raws deploy`: Synth and deploy CDK stacks to AWS.
  - `raws test`: Local testing with DynamoDB Local and API Gateway mocks.

- **Project Structure**:

  ```
  serverless-rails/
  ├── infra/
  │   ├── api/
  │   │   ├── domains/
  │   │   │   └── users/
  │   │   │       ├── domain.js        # Domain definition
  │   │   │       ├── generated/       # Build artifacts
  │   │   │       │   ├── USER_CREATE.request.vtl
  │   │   │       │   ├── USER_CREATE.response.vtl
  │   │   │       │   ├── USER_GET.request.vtl
  │   │   │       │   ├── USER_GET.response.vtl
  │   │   │       │   ├── USER_UPDATE.request.vtl
  │   │   │       │   ├── USER_UPDATE.response.vtl
  │   │   │       │   └── models.json
  │   │   │       └── custom/          # Hand-written overrides
  │   │   ├── lib/
  │   │   │   └── domain.js            # Base class for domains
  │   │   ├── routes.json              # Auto-generated route manifest
  │   │   └── stack.js                 # CDK API stack
  │   ├── etl/
  │   ├── static-web-hosting/
  │   │   └── stack.js
  │   └── shared/
  │       ├── dynamodb-stack.js
  │       └── cognito-stack.js
  ├── frontend/
  │   ├── js/
  │   │   ├── auth.js                 # Cognito abstraction layer
  │   │   ├── crud-manager.js         # Rails-inspired API client
  │   │   └── routes.json              # Copied from build
  │   ├── pages/
  │   │   ├── index.html              # Landing page
  │   │   ├── login.html              # Sign in
  │   │   ├── signup.html             # Sign up
  │   │   └── profile.html            # User profile management
  │   └── css/
  │       └── app.css
  ├── build/
  │   ├── generate-vtl.js              # Reads domain.js, outputs VTL
  │   ├── generate-routes-manifest.js  # Creates routes.json
  │   └── validate-domains.js
  └── package.json
  ```

- **Local Development**: Use LocalStack or AWS SAM for simulating services; hot-reload VTL changes.

## Domain Definition Pattern

```javascript
// infra/api/domains/users/domain.js
const { Domain } = require("../../lib/domain");

class UserDomain extends Domain {
  // All operations require auth by default
  auth = { required: true };

  constructor() {
    super();
    this.primaryKey = "userId"; // Will use Cognito sub
  }

  operations() {
    return {
      // Create user profile after Cognito signup
      CREATE: this.putOperation({
        auth: { required: false }, // Public endpoint for post-signup
        attributes: {
          email: this.string({ required: true, validation: "email" }),
          name: this.string({ required: true, minLength: 2, maxLength: 100 }),
          timezone: this.string({ default: "UTC" }),
          locale: this.enum(["en-US", "es-ES", "fr-FR"], { default: "en-US" }),
          preferences: this.map({
            emailNotifications: this.boolean({ default: true }),
            theme: this.enum(["light", "dark", "auto"], { default: "auto" }),
          }),
          createdAt: this.timestamp({ auto: true }),
          updatedAt: this.timestamp({ auto: true }),
        },
      }),

      // Get own profile
      GET: this.getOperation({
        projections: [
          "userId",
          "email",
          "name",
          "timezone",
          "locale",
          "preferences",
          "createdAt",
        ],
      }),

      // Update profile
      UPDATE: this.updateOperation({
        allowedAttributes: ["name", "timezone", "locale", "preferences"],
      }),

      // Soft delete (keeps Cognito account)
      DELETE: this.deleteOperation({
        softDelete: true,
      }),
    };
  }

  // Ensure users can only access their own profile
  validateGet(request) {
    if (request.userId !== request.auth.sub) {
      throw new ForbiddenError("Can only access your own profile");
    }
  }

  validateUpdate(request) {
    if (request.userId !== request.auth.sub) {
      throw new ForbiddenError("Can only update your own profile");
    }
  }

  validateDelete(request) {
    if (request.userId !== request.auth.sub) {
      throw new ForbiddenError("Can only delete your own profile");
    }
  }
}

module.exports = UserDomain;
```

## Base Domain Class

```javascript
// infra/api/lib/domain.js
class Domain {
  // Default: all operations require auth
  auth = { required: true };

  constructor() {
    this.primaryKey = "id";
    this.domainName = this.constructor.name
      .replace(/Domain$/, "")
      .toUpperCase();
  }

  get config() {
    return {
      domain: this.domainName,
      auth: this.auth,
      operations: this.operations(),
    };
  }

  // Subclasses must implement
  operations() {
    throw new Error("Subclasses must implement operations()");
  }

  // Operation helpers
  putOperation(config = {}) {
    return {
      operation: "put",
      pk: `${this.domainName}#\${${this.primaryKey}}`,
      sk: config.sk || "A",
      auth: { ...this.auth, ...config.auth },
      ...config,
    };
  }

  getOperation(config = {}) {
    return {
      operation: "get",
      pk: `${this.domainName}#\${${this.primaryKey}}`,
      sk: "A",
      auth: { ...this.auth, ...config.auth },
      ...config,
    };
  }

  updateOperation(config = {}) {
    return {
      operation: "update",
      pk: `${this.domainName}#\${${this.primaryKey}}`,
      sk: "A",
      auth: { ...this.auth, ...config.auth },
      ...config,
    };
  }

  deleteOperation(config = {}) {
    return {
      operation: "delete",
      pk: `${this.domainName}#\${${this.primaryKey}}`,
      sk: "A",
      auth: { ...this.auth, ...config.auth },
      ...config,
    };
  }

  queryOperation(config = {}) {
    return {
      operation: "query",
      indexName: config.index,
      keyCondition: {
        pk: config.pk,
        sk: config.sk || { operator: "begins_with", value: "" },
      },
      limit: config.limit || 20,
      scanIndexForward: config.scanIndexForward !== false,
      auth: { ...this.auth, ...config.auth },
      ...config,
    };
  }

  // Type helpers
  string(config = {}) {
    return { type: "string", ...config };
  }

  number(config = {}) {
    return { type: "number", ...config };
  }

  boolean(config = {}) {
    return { type: "boolean", ...config };
  }

  enum(values, config = {}) {
    return { type: "string", enum: values, ...config };
  }

  timestamp(config = {}) {
    return { type: "timestamp", ...config };
  }

  map(schema, config = {}) {
    return { type: "map", schema, ...config };
  }

  // Validation hooks (override in subclasses)
  validateCreate(request) {}
  validateGet(request) {}
  validateUpdate(request) {}
  validateDelete(request) {}
}

module.exports = { Domain };
```

## Authentication Flow

1. **User signs up** → Cognito account created
2. **Email verification** → Confirms account
3. **User signs in** → Receives JWT tokens
4. **Create profile** → `POST /users` with user data (public endpoint)
5. **Access profile** → All subsequent calls require JWT in Authorization header

## DynamoDB Schema

```
User Profile (main record):
PK: USER#<cognito-sub>
SK: A
Attributes: email, name, timezone, locale, preferences, createdAt, updatedAt, deletedAt

Future expansion:
PK: USER#<cognito-sub>
SK: SETTINGS        # User settings
SK: SESSION#<id>    # Active sessions
SK: AUDIT#<timestamp> # Audit log
```

## Generated Routes

For the users domain:

```json
{
  "users": {
    "base": "/users",
    "operations": {
      "standard": [
        { "method": "POST", "path": "/users", "auth": false },
        { "method": "GET", "path": "/users/{userId}", "auth": true },
        { "method": "PUT", "path": "/users/{userId}", "auth": true },
        { "method": "DELETE", "path": "/users/{userId}", "auth": true }
      ],
      "custom": []
    }
  }
}
```

## Frontend Auth Abstraction

```javascript
// frontend/js/auth.js
class Auth {
  constructor(config = {}) {
    this.userPoolId = config.userPoolId;
    this.clientId = config.clientId;
    this.region = config.region || "us-east-1";
  }

  async signUp(email, password) {
    // Returns: { userId, userConfirmed }
  }

  async confirmSignUp(email, code) {
    // Confirms email with code
  }

  async signIn(email, password) {
    // Returns: { accessToken, idToken, refreshToken }
  }

  async signOut() {
    // Clears all tokens
  }

  async getSession() {
    // Returns current session or refreshes
  }

  getIdToken() {
    // Returns JWT for API calls
  }
}

window.auth = new Auth({
  userPoolId: process.env.USER_POOL_ID,
  clientId: process.env.CLIENT_ID,
});
```

## Build Process

```bash
# 1. Validate all domains
node build/validate-domains.js

# 2. Generate VTL templates
node build/generate-vtl.js

# 3. Generate routes manifest
node build/generate-routes-manifest.js

# 4. Copy routes to frontend
cp infra/api/routes.json frontend/js/

# 5. Deploy with CDK
cdk deploy --all
```

## Key Conventions

1. **Domain names are UPPERCASE**: `USER#`, `TODO#`, `ORDER#`
2. **Route paths are lowercase plural**: `/users`, `/todos`, `/orders`
3. **Primary key field matches domain**: `userId`, `todoId`, `orderId`
4. **All operations require auth by default**: Override with `auth: { required: false }`
5. **Validation happens in domain class**: Not in VTL templates
6. **Generated code is readable**: Can be debugged and understood
7. **Custom VTL goes in `custom/`**: Override specific operations when needed

## CDK Integration

The CDK stack reads domain configs and:

- Creates API Gateway routes
- Wires Cognito authorizer based on `auth` settings
- Maps routes to VTL templates
- Sets up request/response models

```javascript
// Auto-wires auth based on domain config
if (operation.auth?.required !== false) {
  route.authorizer = cognitoAuthorizer;
}
```

## Getting Started (v0.1)

Focus on validating the core: Create and retrieve a user with only VTL (no Lambda).

1. **Day 1: Manual VTL Setup**

   - Hand-write basic VTL templates for CREATE/GET user.
   - Deploy via CDK to test direct DynamoDB integration.

2. **Day 2: Magic Helper Implementation**

   - Build `registerCrudRoutes` to auto-wire routes, VTL, and Cognito auth.
   - Test with Cognito CLI (for tokens) + curl (e.g., POST /users with JWT).

3. **What NOT to Build Yet**:

   - No frontend/UI.
   - No generators (use manual templates as targets).
   - No domain classes/models (focus on infra/API).
   - No advanced features like search or ETL (add after CRUD validation).

4. **Testing Strategy**:
   - Use Cognito CLI for auth tokens.
   - Curl for API calls; verify DynamoDB items directly.
   - Success Criteria: Create and retrieve a user purely via VTL, with auth enforced.

## Roadmap for v0.1 and Beyond

- **v0.1 Focus**: Core validation (VTL CRUD + auth), manual templates, and magic helper—proving the hypothesis before complexity.
- **Next**: Integrate FDA/RxNorm data ETL; add generators building on manual examples; full-stack web (React/Amplify).
- **Future**: Open-source post-v0.5; community contribs for extensions (e.g., ML via SageMaker).
- **Risks & Mitigations**: VTL quirks—document in examples; scale testing—include load scripts.

Feedback welcome—let's iterate! For data ETL details (FDA/RxNorm), provide sources/requirements next.
