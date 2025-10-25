# Decision Record: Delta Table Registration via IaC (Glue 5.0 + Athena v3)

## Context

Historically, our GOLD-layer Glue tables were created via crawlers to automatically infer schema and partitions. However, this approach caused repeated issues with Delta Lake tables:
- Crawlers registered Delta tables as `classification=parquet` instead of `table_type='DELTA'`
- Athena queries returned empty results despite data existing in S3
- The process was non-deterministic and not suitable for CI/CD promotion (dev → stage → prod)
- Symlink manifest tables created confusion between Athena v2 and v3 formats

## Decision

We are **eliminating Glue Crawlers for GOLD Delta Lake tables** and defining all Delta tables directly via **Infrastructure as Code (CDK)**.

This represents the **simplest, most modern, AWS-native approach** to Delta Lake with Glue 5.0 + Athena v3 using only AWS primitives:

### The Canonical Stack
- **S3** → Raw data + Delta transaction log (`_delta_log/`)
- **Glue ETL 5.0** → Spark 3.5 runtime with Delta Lake 3.0 built-in (`--datalake-formats=delta`)
- **Glue Data Catalog (CfnTable)** → Declarative Delta table registration
- **Athena v3 (Trino)** → Native Delta SQL engine (no manifests needed)
- **CloudFormation / CDK** → End-to-end IaC

✅ 100% serverless, pay-per-query, CloudFormation-native
✅ No Databricks, EMR, Lake Formation permissions, or custom Lambda code
✅ Glue's built-in Delta runtime + Athena's native reader eliminate complexity

Each GOLD dataset stack now:
1. Writes to a Delta Lake table in S3 (`/gold/<dataset>/`)
2. Registers the Glue table automatically during deployment using `glue.CfnTable` (first-class CloudFormation resource)
3. Sets the following metadata to ensure Athena v3 compatibility:
   ```json
   {
     "classification": "delta",
     "table_type": "DELTA",
     "EXTERNAL": "TRUE"
   }
   ```
4. Uses correct Parquet I/O formats (not symlink text formats):
   ```
   InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
   OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
   ```

## Consequences

### Benefits
- **Deterministic**: Table registration happens at deployment time, not on-demand
- **CI/CD Ready**: Same IaC code promotes across environments
- **No Manual Steps**: Zero SQL or CLI commands required
- **Schema Evolution**: Delta transaction log manages schema automatically
- **Athena v3 Native**: Direct Delta Lake reads without manifest generation
- **Version Control**: Table definitions tracked in Git like all other infrastructure

### Trade-offs
- Schema must be evolved through Delta writes (not crawler inference)
- Table metadata updates require stack deployment (not crawler re-runs)
- Initial learning curve for teams accustomed to crawler-based workflows

## Why This Pattern Is Optimal

### Architectural Simplicity

This is **the simplest possible Delta Lake implementation** using only AWS primitives. Compared to alternatives:

| Alternative Pattern              | Why It's More Complex                                    |
|----------------------------------|----------------------------------------------------------|
| **Glue Job + Crawler**           | Crawler misidentifies Delta as Parquet; creates wrong tables |
| **AwsCustomResource / Lambda**   | Procedural logic, IAM complexity, non-deterministic      |
| **Databricks / EMR Delta**       | External control plane + additional costs + IAM setup   |
| **Lake Formation Delta**         | Over-engineered unless row-level permissions needed      |
| **CDK `CfnTable` (this pattern)** | Declarative, portable, cost-free, version-controlled     |

### Operational Characteristics

| Aspect            | Result                                                   |
|-------------------|----------------------------------------------------------|
| **Cost**          | Pennies/day for S3 + Glue catalog + occasional ETL runs |
| **Maintenance**   | Near-zero; schema evolves automatically via Delta log    |
| **Portability**   | Full; deploys identically across dev/stage/prod         |
| **Observability** | CloudWatch + Spark UI enabled out-of-box                |
| **Time-to-prod**  | Minutes; no cluster bootstrap or external dependencies   |

### When You'd Outgrow This

Only move beyond this pattern if you need:
- **Streaming upserts** → Glue Streaming or Kinesis Data Analytics
- **Cross-table transactions** → Lake Formation governed tables
- **Fine-grained row/column permissions** → Lake Formation or Databricks Unity Catalog
- **Python UDF-heavy workloads** → EMR Serverless

Otherwise, this pattern scales to billions of rows and hundreds of datasets.

## Implementation Pattern

### CDK Stack (GoldStack.js) - Production Hardened

```javascript
const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");

// Gold table name (hyphens → underscores for Glue)
const goldTableName = dataset.replace(/-/g, '_');

// Build paths using convention
const goldBasePath = `s3://${bucketName}/gold/${dataset}/`;

// Defensive validation for S3 path
if (!goldBasePath.startsWith('s3://')) {
  throw new Error(`goldBasePath must be an S3 URI, got: ${goldBasePath}`);
}

// Register Delta Lake table using pure CDK (first-class CloudFormation resource)
const goldTable = new glue.CfnTable(this, 'GoldTable', {
  databaseName: goldDatabase,
  catalogId: this.account,  // Resolves at deploy-time for multi-account compatibility
  tableInput: {
    name: goldTableName,
    description: `Gold layer Delta table for ${dataset} with SCD Type 2 temporal versioning`,

    // Note: tableType is inferred by Glue when table_type='DELTA', but we keep it explicit
    tableType: 'EXTERNAL_TABLE',

    // CRITICAL: These parameters enable Athena Engine v3 native Delta reads
    parameters: {
      'classification': 'delta',      // Identifies table format as Delta Lake
      'table_type': 'DELTA',           // Enables Athena v3 native Delta reads
      'EXTERNAL': 'TRUE',              // Marks table as externally managed
      'external': 'TRUE'               // Duplicate for AWS normalization across regions
    },

    // Storage descriptor with correct Parquet I/O formats for Delta Lake
    // Schema Evolution Note: Glue 5.0 + Delta supports automatic catalog updates via:
    //   spark.databricks.delta.schema.autoMerge.enabled = true
    storageDescriptor: {
      location: goldBasePath,
      inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
      outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
      serdeInfo: {
        serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
      }
      // columns array omitted - Delta Lake manages schema via transaction log
    }
  }
});

// Ensure table is created after job to prevent race conditions
goldTable.node.addDependency(goldJob);
goldTable.node.addDependency(dataWarehouseBucket);
```

### Production Hardening Features

1. **Pure CloudFormation**: Uses `glue.CfnTable` - first-class resource, not Lambda-backed
2. **Multi-Account Safe**: `this.account` resolves at deploy-time (not synth-time)
3. **Region Normalization**: Dual `EXTERNAL`/`external` handles AWS casing inconsistencies
4. **Defensive Validation**: S3 URI check catches config errors at synth time
5. **Explicit Dependencies**: Prevents race conditions during stack creation
6. **Schema Evolution**: Delta manages schema; comment documents auto-merge capability
7. **Fully Declarative**: No procedural logic, no IAM complexity
8. **Environment Portable**: Promotes identically across dev/stage/prod

## Validation

After deployment, verify table registration:

```bash
# Check Glue catalog
aws glue get-table --database-name pp_dw_gold --name fda_all_ndcs \
  --query 'Table.Parameters' --output json

# Expected output:
{
  "classification": "delta",
  "table_type": "DELTA",
  "EXTERNAL": "TRUE"
}

# Query in Athena (Engine v3)
SELECT COUNT(*) FROM pp_dw_gold.fda_all_ndcs;
```

## Migration from Crawler-Based Pattern

For existing gold datasets using crawlers:

1. **Update Stack**: Replace `CfnCrawler` with `glue.CfnTable` pattern above
2. **Remove Crawler References**: Delete `crawler_name` from job arguments
3. **Update Job Script**: Remove crawler-related output messages
4. **Delete Old Table**: `aws glue delete-table --database pp_dw_gold --name <old_table>`
5. **Deploy Stack**: Table will be recreated with correct metadata via CloudFormation
6. **Validate**: Run Athena query to confirm data is accessible

## Status

- **Implemented**: `fda-all-ndcs` (2025-10-25)
- **Pending**: `rxnorm-products`, `rxnorm-ndc-mappings`, `drug-product-codesets`

---

**Decision Date**: 2025-10-25
**Author**: RAWS Architecture Team
**Status**: Accepted
