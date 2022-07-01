const join = require('lodash/join');
const map = require('lodash/map');
const mapValues = require('lodash/mapValues');
const replace = require('lodash/replace');

const { DEFAULT_LOAD_EXTENSIONS } = require('../common/MigrationsLoader');
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
      return _removeExtensionFromMigrationNames(tableName, schemaName, trxOrKnex);
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
    }
  );
}

function _createMigrationLockTable(tableName, schemaName, trxOrKnex) {
  return getSchemaBuilder(trxOrKnex, schemaName).createTable(
    tableName,
    function (t) {
      t.increments('index').primary();
      t.integer('is_locked');
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

function _removeExtensionFromMigrationNames(tableName, schemaName, trxOrKnex) {
  const loadExtensionsPartOfRegexPattern = join(
    map(
      DEFAULT_LOAD_EXTENSIONS,
      (value) => '\\' + value),
    '|');
  const loadExtensionsRegex = new RegExp('(.*)(' + loadExtensionsPartOfRegexPattern + ')$');

  return trxOrKnex
    .select('id', 'name')
    .from(getTableName(tableName, schemaName))
    .then((data) => {
      return data.filter((row) => loadExtensionsRegex.test(row.name));
    })
    .then((data) => {
      return data.map((row) => {
        return mapValues(row, (value, key) =>
          key === 'name'
            ? replace(value, loadExtensionsRegex, '$1')
            : value
        )
      });
    })
    .then((data) => {
      return trxOrKnex.transaction(transaction => {
        return Promise.all(data.map((row) => trxOrKnex(getTableName(tableName, schemaName))
          .where({ 'id': row.id })
          .update({ 'name': row.name })
          .transacting(transaction))).then(transaction.commit).catch(transaction.rollback);
      });
    });
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
