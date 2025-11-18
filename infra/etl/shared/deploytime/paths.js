/**
 * Deploy-time filesystem paths helper for CDK stacks.
 *
 * IMPORTANT: This module is for CDK/deploy-time use ONLY.
 * Runtime code (Glue jobs, Lambda handlers) must never use filesystem paths.
 * Runtime code should only work with S3 URIs and /tmp as needed.
 *
 * Usage from dataset stacks (at infra/etl/datasets/{layer}/{dataset}/):
 *   const { sharedRuntimePath, glueScriptPath } = require('../../../shared/deploytime/paths');
 *
 *   // For shared runtime libraries
 *   sharedRuntimePath(__dirname, 'temporal')
 *
 *   // For dataset-local glue scripts
 *   glueScriptPath(__dirname)
 */

const path = require("path");

/**
 * General-purpose relative path builder from a stack directory.
 *
 * @param {string} stackDir - The stack's __dirname
 * @param {...string} segments - Path segments to join
 * @returns {string} The joined absolute path
 */
function relativeFrom(stackDir, ...segments) {
  return path.join(stackDir, ...segments);
}

/**
 * Returns the absolute path to the shared runtime root directory.
 *
 * From dataset stacks at level 4 (infra/etl/datasets/{layer}/{dataset}/):
 *   - Go up 3 levels: ../ (to layer) ../ (to datasets) ../ (to etl)
 *   - Then down to: shared/runtime
 *
 * @param {string} stackDir - The stack's __dirname
 * @returns {string} Path to infra/etl/shared/runtime
 */
function sharedRuntimeRoot(stackDir) {
  return relativeFrom(
    stackDir,
    "..",      // → layer (bronze/silver/gold)
    "..",      // → datasets
    "..",      // → etl
    "shared",
    "runtime"
  );
}

/**
 * Returns the absolute path to a specific shared runtime library subfolder.
 *
 * Example:
 *   sharedRuntimePath(__dirname, 'temporal')
 *     → /path/to/infra/etl/shared/runtime/temporal
 *
 *   sharedRuntimePath(__dirname, 'https_zip')
 *     → /path/to/infra/etl/shared/runtime/https_zip
 *
 * @param {string} stackDir - The stack's __dirname
 * @param {string} subfolder - The runtime library subfolder name
 * @returns {string} Path to the runtime library
 */
function sharedRuntimePath(stackDir, subfolder) {
  return path.join(sharedRuntimeRoot(stackDir), subfolder);
}

/**
 * Returns the absolute path to a dataset's local glue/ directory.
 *
 * Example:
 *   glueScriptPath(__dirname)
 *     → /path/to/infra/etl/datasets/{layer}/{dataset}/glue
 *
 * @param {string} stackDir - The stack's __dirname
 * @returns {string} Path to the dataset's glue directory
 */
function glueScriptPath(stackDir) {
  return path.join(stackDir, "glue");
}

/**
 * Returns the absolute path to shared orchestration lambdas.
 *
 * From orchestration stacks at level 4 (infra/etl/orchestrations/{freq}/{name}/):
 *   - Go up 3 levels to etl root
 *   - Then down to: orchestrations/shared/lambdas
 *
 * @param {string} stackDir - The orchestration stack's __dirname
 * @returns {string} Path to infra/etl/orchestrations/shared/lambdas
 */
function sharedLambdaPath(stackDir) {
  return relativeFrom(
    stackDir,
    "..",             // → freq (daily/monthly)
    "..",             // → orchestrations
    "shared",
    "lambdas"
  );
}

/**
 * For the root-level EtlCoreStack, returns path to shared runtime.
 *
 * From EtlCoreStack.js at infra/etl/:
 *   - Directly access: shared/runtime
 *
 * @param {string} stackDir - The root stack's __dirname
 * @returns {string} Path to infra/etl/shared/runtime
 */
function rootSharedRuntimePath(stackDir) {
  return path.join(stackDir, "shared", "runtime");
}

/**
 * For factory/utility code, computes path to a dataset's glue directory.
 *
 * From shared/deploytime/ (level 3):
 *   - Go up 2 levels to etl root
 *   - Then down to: datasets/{layer}/{dataset}/glue
 *
 * @param {string} layer - The layer name (bronze/silver/gold)
 * @param {string} dataset - The dataset name
 * @returns {string} Path to the dataset's glue directory
 */
function datasetGluePath(layer, dataset) {
  // From shared/deploytime/, go up to etl root
  const etlRoot = path.join(__dirname, "..", "..");
  return path.join(etlRoot, "datasets", layer, "glue");
}

module.exports = {
  relativeFrom,
  sharedRuntimeRoot,
  sharedRuntimePath,
  glueScriptPath,
  sharedLambdaPath,
  rootSharedRuntimePath,
  datasetGluePath
};
