const glue = require('aws-cdk-lib/aws-glue');

/**
 * Helper function to create Parquet tables with minimal schema
 * Tables are pre-created so crawlers only update schema/partitions
 * Supports partition projection configuration for faster query performance
 * 
 * @param {Object} scope - CDK construct scope
 * @param {string} id - CDK construct ID
 * @param {Object} props - Table configuration properties
 * @param {string} props.dbName - Glue database name
 * @param {string} props.s3Location - S3 location for table data
 * @param {string} props.tableName - Table name
 * @param {Array} props.partitionKeys - Array of partition key objects with name and type
 * @param {boolean} props.enableProjection - Enable partition projection
 * @param {Object} props.projectionConfig - Projection configuration per partition key
 * 
 * @example
 * // Date partition projection
 * makeParquetTable(this, 'MyTable', {
 *   dbName: 'my_database',
 *   s3Location: 's3://bucket/data/',
 *   tableName: 'my_table',
 *   partitionKeys: [{ name: 'version_date', type: 'date' }],
 *   enableProjection: true,
 *   projectionConfig: {
 *     version_date: {
 *       type: 'date',
 *       format: 'yyyy-MM-dd',
 *       range: '2025-01-01,NOW'
 *     }
 *   }
 * });
 * 
 * @example
 * // Enum partition projection
 * makeParquetTable(this, 'MyTable', {
 *   dbName: 'my_database',
 *   s3Location: 's3://bucket/data/',
 *   tableName: 'my_table',
 *   partitionKeys: [{ name: 'region', type: 'string' }],
 *   enableProjection: true,
 *   projectionConfig: {
 *     region: {
 *       type: 'enum',
 *       values: 'us-east-1,us-west-2,eu-west-1'
 *     }
 *   }
 * });
 */
function makeParquetTable(scope, id, props) {
  const { 
    dbName, 
    s3Location, 
    tableName, 
    partitionKeys = [], 
    enableProjection = false,
    projectionConfig = {} // Optional projection config per partition key
  } = props;
  
  // Strict validation for v0.1 testing
  if (partitionKeys.length > 0 && enableProjection && Object.keys(projectionConfig).length === 0) {
    throw new Error(`Table ${tableName}: enableProjection is true but projectionConfig is required. Provide projection settings for partition keys.`);
  }
  
  // Build table parameters
  const parameters = {
    'EXTERNAL': 'TRUE',
    'parquet.compression': 'ZSTD'
  };
  
  // Add partition projection if enabled
  if (enableProjection && partitionKeys.length > 0) {
    parameters['projection.enabled'] = 'true';
    
    // Build projection parameters and location template
    let template = s3Location;
    partitionKeys.forEach(key => {
      const config = projectionConfig[key.name] || {};
      
      // Apply default or custom projection settings
      if (config.type || key.type === 'date') {
        parameters[`projection.${key.name}.type`] = config.type || 'date';
        parameters[`projection.${key.name}.format`] = config.format || 'yyyy-MM-dd';
        parameters[`projection.${key.name}.range`] = config.range || '2025-01-01,NOW';
      } else {
        parameters[`projection.${key.name}.type`] = config.type || 'enum';
        // Ensure enum values is a comma-separated string
        let enumValues = config.values || '';
        if (Array.isArray(enumValues)) {
          enumValues = enumValues.join(',');
        }
        parameters[`projection.${key.name}.values`] = enumValues;
      }
      
      template += `${key.name}=\${${key.name}}/`;
    });
    
    parameters['storage.location.template'] = template;
  }
  
  return new glue.CfnTable(scope, id, {
    catalogId: scope.account,
    databaseName: dbName,
    tableInput: {
      name: tableName,
      storageDescriptor: {
        location: s3Location,
        // Let Glue infer format from files (Parquet auto-detected)
        serdeInfo: {
          serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        },
        columns: [], // Minimal columns - crawler will add the rest
        compressed: true,
        storedAsSubDirectories: true // Hive-style partitioned directories
      },
      partitionKeys: partitionKeys,
      tableType: 'EXTERNAL_TABLE',
      parameters: parameters
    }
  });
}

module.exports = { makeParquetTable };