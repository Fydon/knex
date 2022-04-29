const { DEFAULT_LOAD_EXTENSIONS } = require('../common/MigrationsLoader');
const forEach = require('lodash/forEach');
const join = require('lodash/join');
const map = require('lodash/map');
const mapValues = require('lodash/mapValues');
const replace = require('lodash/replace');
const {
  getTable,
  getLockTableName,
  getLockTableNameWithSchema,
  getTableName,
} = require('./table-resolver');

function ensureTable(tableName, schemaName, trxOrKnex) {
  const lockTable = getLockTableName(tableName);
  return getSchemaBuilder(trxOrKnex, schemaName)
    .hasTable(tableName)
    .then((exists) => {
      return !exists && _createMigrationTable(tableName, schemaName, trxOrKnex);
    })
    .then(() => {
      return _shouldMigrationTableDefinitionBeUpdated(tableName, schemaName, trxOrKnex);
    })
    .then((shouldUpdate) => {
      return !shouldUpdate && _updateMigrationTableToLatestDefinition(tableName, schemaName, trxOrKnex);
    })
    .then(() => {
      return _updateMigrationsInMigrationTableToLatestVersion(tableName, schemaName, trxOrKnex);
    })
    .then(() => {
      return getSchemaBuilder(trxOrKnex, schemaName).hasTable(lockTable);
    })
    .then((exists) => {
      return (
        !exists && _createMigrationLockTable(lockTable, schemaName, trxOrKnex)
      );
    })
    .then(() => {
      return _shouldMigrationLockTableDefinitionBeUpdated(lockTable, schemaName, trxOrKnex);
    })
    .then((shouldUpdate) => {
      return !shouldUpdate && _updateMigrationLockTableToLatestDefinition(lockTable, schemaName, trxOrKnex);
    })
    .then(() => {
      return getTable(trxOrKnex, lockTable, schemaName).select('*');
    })
    .then((data) => {
      return (
        !data.length && _insertLockRowIfNeeded(tableName, schemaName, trxOrKnex)
      );
    });
}

function _createMigrationTable(tableName, schemaName, trxOrKnex) {
  return getSchemaBuilder(trxOrKnex, schemaName).createTable(
    getTableName(tableName),
    function (t) {
      t.increments();
      t.string('name');
      t.integer('batch');
      t.timestamp('migration_time');
      t.integer('version').defaultTo(1);
    }
  );
}

function _createMigrationLockTable(tableName, schemaName, trxOrKnex) {
  return getSchemaBuilder(trxOrKnex, schemaName).createTable(
    tableName,
    function (t) {
      t.increments('index').primary();
      t.integer('is_locked');
      t.integer('version').defaultTo(1);
    }
  );
}

function _insertLockRowIfNeeded(tableName, schemaName, trxOrKnex) {
  const lockTableWithSchema = getLockTableNameWithSchema(tableName, schemaName);
  return trxOrKnex
    .select('*')
    .from(lockTableWithSchema)
    .then((data) => {
      return !data.length
        ? trxOrKnex.from(lockTableWithSchema).insert({ is_locked: 0 })
        : null;
    });
}

function _shouldMigrationLockTableDefinitionBeUpdated(tableName, schemaName, trxOrKnex) {
  return getSchemaBuilder(trxOrKnex, schemaName).hasColumn(getTableName(tableName, schemaName), 'version');
}

function _shouldMigrationTableDefinitionBeUpdated(tableName, schemaName, trxOrKnex) {
  return getSchemaBuilder(trxOrKnex, schemaName).hasColumn(getTableName(tableName, schemaName), 'version');
}

function _updateMigrationLockTableToLatestDefinition(tableName, schemaName, trxOrKnex) {
  return getSchemaBuilder(trxOrKnex, schemaName).alterTable(
    getTableName(tableName, schemaName),
    function (t) {
      t.integer('version').defaultTo(1);
    }
  );
}

function _updateMigrationsInMigrationTableToLatestVersion(tableName, schemaName, trxOrKnex) {
  // version 1 to 2
  const loadExtensionsPartOfRegexPattern = join(
    map(
      DEFAULT_LOAD_EXTENSIONS,
      (value) => '\\' + value),
    '|');
  const loadExtensionsRegex = new RegExp('(.*)(' + loadExtensionsPartOfRegexPattern + ')$');

  return trxOrKnex
    .select('id', 'name')
    .from(getTableName(tableName, schemaName))
    .where('version', '<', 2)
    .then((data) => {
      return map(data, (row) => {
        return mapValues(row, (value, key) =>
          key === 'name'
            ? replace(value, loadExtensionsRegex, '$1')
            : value
        )
      });
    })
    .then((data) => {
      forEach(data, (row) => trxOrKnex
        .from(getTableName(tableName, schemaName))
        .update({ 'name': row.name, 'version': 2 })
        .where('id', '=', row.id))
    });
}

function _updateMigrationTableToLatestDefinition(tableName, schemaName, trxOrKnex) {
  return getSchemaBuilder(trxOrKnex, schemaName).alterTable(
    getTableName(tableName, schemaName),
    function (t) {
      t.integer('version').defaultTo(1);
    }
  );
}

//Get schema-aware schema builder for a given schema nam
function getSchemaBuilder(trxOrKnex, schemaName) {
  return schemaName
    ? trxOrKnex.schema.withSchema(schemaName)
    : trxOrKnex.schema;
}

module.exports = {
  ensureTable,
  getSchemaBuilder,
};
