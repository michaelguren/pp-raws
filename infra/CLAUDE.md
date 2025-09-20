# Infrastructure Development Guidelines

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