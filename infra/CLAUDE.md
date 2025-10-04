# Infrastructure Architecture

## Multi-App CDK Structure

This project uses a **domain-driven CDK architecture** where each major domain is an independent CDK app. This prevents any single `index.js` from becoming too large and keeps domains loosely coupled.

```
infra/
├── index.js           # Reference example for cross-domain dependencies (not used)
├── etl/              # ETL domain - Independent CDK app
│   ├── index.js      # ETL CDK app entrypoint
│   ├── cdk.json      # CDK config: "app": "node index.js"
│   ├── cdk.out/      # ETL-specific synth output (gitignored)
│   └── ...
├── api/              # API domain (future) - Independent CDK app
│   ├── index.js      # API CDK app entrypoint
│   ├── cdk.json      # CDK config: "app": "node index.js"
│   ├── cdk.out/      # API-specific synth output (gitignored)
│   └── ...
└── frontend/         # Frontend domain (future) - Independent CDK app
    ├── index.js      # Frontend CDK app entrypoint
    ├── cdk.json      # CDK config: "app": "node index.js"
    ├── cdk.out/      # Frontend-specific synth output (gitignored)
    └── ...
```

**Key principle**: Each domain has its own `cdk.json` pointing to its own `index.js`. No root `cdk.json` exists - domains are completely independent.

## Deployment Options

### Option 1: Deploy via NPM Scripts (Recommended)

Each domain has convenience npm scripts in root `package.json`:

```bash
# ETL Infrastructure
npm run etl:deploy              # Deploy all ETL stacks
npm run etl:deploy:core         # Deploy only core ETL infrastructure
npm run etl:diff                # See what would change

# Future: API Infrastructure
npm run api:deploy              # Deploy all API stacks
npm run api:diff                # See what would change

# Future: Frontend Infrastructure
npm run frontend:deploy         # Deploy all frontend stacks
```

### Option 2: Deploy from Domain Directory

Navigate to domain and use CDK commands directly:

```bash
cd infra/etl
cdk deploy --all                # Deploy all ETL stacks
cdk deploy pp-dw-etl-core       # Deploy specific stack
cdk deploy pp-dw-etl-fda-nsde   # Deploy specific dataset
cdk diff --all                  # See what would change
cdk synth                       # Synthesize CloudFormation templates
```

### Cross-Domain Dependencies (Future)

When you need cross-domain dependencies (e.g., API needs ETL bucket):

1. Export values from source stack (ETL):
   ```javascript
   new cdk.CfnOutput(this, "DataBucketName", {
     value: bucket.bucketName,
     exportName: "pp-dw-etl-data-bucket"
   });
   ```

2. Import in dependent stack (API):
   ```javascript
   const bucketName = cdk.Fn.importValue("pp-dw-etl-data-bucket");
   const bucket = s3.Bucket.fromBucketName(this, "EtlBucket", bucketName);
   ```

See `infra/index.js` for reference implementation (not actively used).

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