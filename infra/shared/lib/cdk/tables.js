const glue = require('aws-cdk-lib/aws-glue');

/**
 * Helper function to create Parquet tables with minimal schema
 * Tables are pre-created so crawlers only update schema/partitions
 * Supports optional partition projection for date-based partitions
 */
function makeParquetTable(scope, id, props) {
  const { dbName, s3Location, tableName, partitionKeys = [], enableProjection = false } = props;
  
  return new glue.CfnTable(scope, id, {
    catalogId: scope.account,
    databaseName: dbName,
    tableInput: {
      name: tableName,
      storageDescriptor: {
        location: s3Location,
        inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        serdeInfo: {
          serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          parameters: {
            'serialization.format': '1'
          }
        },
        columns: [], // Minimal columns - crawler will add the rest
        compressed: true,
        storedAsSubDirectories: false
      },
      partitionKeys: partitionKeys, // Partition keys at table level, not in storageDescriptor
      tableType: 'EXTERNAL_TABLE',
      parameters: enableProjection ? {
        'EXTERNAL': 'TRUE',
        'parquet.compression': 'ZSTD',
        'projection.enabled': 'true',
        'projection.version.type': 'date',
        'projection.version.format': 'yyyy-MM-dd',
        'projection.version.range': '2025-01-01,NOW',
        'projection.version_date.type': 'date',
        'projection.version_date.format': 'yyyy-MM-dd', 
        'projection.version_date.range': '2025-01-01,NOW',
        'storage.location.template': s3Location + 'version=${version}/version_date=${version_date}/'
      } : {
        'EXTERNAL': 'TRUE',
        'parquet.compression': 'ZSTD'
      }
    }
  });
}

module.exports = { makeParquetTable };