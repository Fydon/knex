const { expect } = require('chai');
const fs = require('fs');
const Knex = require('../../../../lib');
const sqliteConfig = require('../../../knexfile').sqlite3;
const { ensureTable } = require('../../../../lib/migrations/migrate/table-creator');

describe('table-creator', () => {
  let knex;
  let databaseTestInstance;

  beforeEach(() => {
    databaseTestInstance = __dirname + '/../../../tableCreatorTestInstance.sqlite3';

    fs.copyFileSync(
      __dirname + '/../../../tableCreatorTest.sqlite3',
      databaseTestInstance
    );

    knex = Knex({
      ...sqliteConfig,
      connection: {
        filename: databaseTestInstance,
      },
      useNullAsDefault: true
    });
  });

  it('removes extension from end of row', async () => {
    await ensureTable('migrations', null, knex);
    const migrationNames = await knex.select('name').from('migrations');

    expect(migrationNames).to.deep.equal([
      { 'name': '1' },
      { 'name': '2' },
      { 'name': '3' },
      { 'name': '4' },
      { 'name': '5' },
      { 'name': '6' },
      { 'name': '7' },
      { 'name': '8' },
      { 'name': '9' },
      { 'name': '10.co ' },
      { 'name': '11.coffee ' },
      { 'name': '12.eg ' },
      { 'name': '13.iced ' },
      { 'name': '14.js ' },
      { 'name': '15.cjs ' },
      { 'name': '16.litcoffee ' },
      { 'name': '17.ls ' },
      { 'name': '18.ts ' }
    ]);
  });

  afterEach(async () => {
    await knex.destroy();

    fs.unlinkSync(databaseTestInstance);
  });
});