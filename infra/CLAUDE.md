# Infrastructure Architecture

## Multi-App CDK Structure

This project uses a **domain-driven CDK architecture** where each major domain has its own CDK app. This prevents any single `index.js` from becoming too large and keeps domains loosely coupled.

```
infra/
├── index.js           # Optional orchestrator for cross-domain deployments
├── cdk.json          # Main CDK config (optional)
├── etl/              # ETL domain
│   ├── index.js      # ETL CDK app
│   ├── cdk.json      # ETL-specific CDK config
│   └── ...
├── api/              # API domain (future)
│   ├── index.js      # API CDK app
│   ├── cdk.json      # API-specific CDK config
│   └── ...
└── frontend/         # Frontend domain (future)
    ├── index.js      # Frontend CDK app
    ├── cdk.json      # Frontend-specific CDK config
    └── ...
```

## Deployment Options

### Option 1: Deploy Individual Domains (Recommended)

Each domain can be deployed independently:

```bash
# ETL Infrastructure
npm run etl:deploy              # Deploy all ETL stacks
npm run etl:deploy:core         # Deploy only core ETL infrastructure
npm run etl:deploy:datasets     # Deploy only dataset stacks

# Future: API Infrastructure
npm run api:deploy              # Deploy all API stacks

# Future: Frontend Infrastructure
npm run frontend:deploy         # Deploy all frontend stacks
```

### Option 2: Deploy from Domain Directory

```bash
cd infra/etl
cdk deploy --all                # Deploy all ETL stacks
cdk deploy pp-dw-etl-core       # Deploy specific stack
cdk diff --all                  # See what would change
```

### Option 3: Cross-Domain Deployment (Optional)

Use the main `infra/index.js` only when you need cross-domain dependencies:

```bash
npm run deploy                  # Deploy everything (if configured in main index.js)
```

## Benefits of This Approach

1. **Scalability**: Each domain's `index.js` stays focused and manageable
2. **Independence**: Teams can work on different domains without conflicts
3. **Flexibility**: Deploy domains separately or together as needed
4. **Clear Boundaries**: Domain separation encourages proper architectural boundaries
5. **Testing**: Each domain can be tested independently

## Adding New Domains

To add a new domain (e.g., `api`):

1. Create the domain directory: `mkdir -p infra/api`
2. Create `infra/api/index.js` with the CDK app
3. Create `infra/api/cdk.json` with `"app": "node index.js"`
4. Add deployment scripts to `package.json`:
   ```json
   "api:deploy": "cd infra/api && cdk deploy --all"
   ```

## Stack Naming Convention

All stacks follow the pattern: `pp-dw-{domain}-{component}`

- ETL: `pp-dw-etl-core`, `pp-dw-etl-fda-nsde`
- API: `pp-dw-api-gateway`, `pp-dw-api-lambdas`
- Frontend: `pp-dw-frontend-hosting`, `pp-dw-frontend-cdn`

## AWS SDK Requirements

**CRITICAL: All infrastructure code MUST use AWS SDK v2**

**JavaScript/Node.js:**
```javascript
// ✅ Correct
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

// ❌ NEVER use
const AWS = require('aws-sdk');
```

**Python:**
```python
# ✅ Use latest boto3
import boto3
```

NEVER use AWS SDK v1 in any infrastructure code.