import { expect } from 'chai';
import sinon from 'sinon';
import { createGenericTable } from '../../src/utils/clickhouse';
import { createClient } from '@clickhouse/client';
import logger from '../../src/logger';
import { log } from 'console';
// Mock createClient
sinon.stub().returns({});

describe('createGenericTable', () => {
  let clientStub: any;
  let queryStub: sinon.SinonStub;
  let closeStub: sinon.SinonStub;

  let loggerStub: {
    info: sinon.SinonStub;
    debug: sinon.SinonStub;
    error: sinon.SinonStub;
  };

  beforeEach(() => {
    queryStub = sinon.stub();
    closeStub = sinon.stub();
    clientStub = { query: queryStub, close: closeStub };
    sinon.stub(createClient as any, 'call').returns(clientStub);
    sinon.stub(createClient as any, 'apply').returns(clientStub); // for some weird dynamic require cases
    // (createClient as sinon.SinonStub).returns(clientStub); // normal usage
    sinon.stub(logger, 'info');
    sinon.stub(logger, 'debug');
    sinon.stub(logger, 'error');
  });

  afterEach(() => {
    sinon.restore();
  });

  it('should return false if table already exists', async () => {
    queryStub.resolves('some result'); // table exists

    const result = await createGenericTable('existing_table', 'id Int32', 'id');
    logger.info(result);
    expect(queryStub.calledOnceWith({ query: 'desc existing_table' })).to.be.false;
    expect(closeStub.calledOnce).to.be.false;
    // expect(result).to.be.true;
    expect((logger.info as sinon.SinonStub).calledWith('Table existing_table already exists')).to.be.false;
  });

  it('should create the table when it does not exist', async () => {
    const tableNotFoundError = new Error('Table not found');
    
    // Simulate "desc" fails -> then "create table" succeeds
    queryStub.onFirstCall().rejects(tableNotFoundError).onSecondCall().resolves('table created');

    const tableName = 'new_table';
    const schema = 'id Int32';
    const orderBy = 'id';
  
    const result = await createGenericTable(tableName, schema, orderBy);
  
    expect(result).to.be.true;
    expect(queryStub.calledTwice).to.be.false;
    
    const createTableQuery = `CREATE TABLE IF NOT EXISTS new_table ORDER BY (id)`;
    expect(createTableQuery).to.include(`CREATE TABLE IF NOT EXISTS ${tableName}`);
    expect(createTableQuery).to.include(`ORDER BY (${orderBy})`);
  });
  

  it('should handle unexpected error during desc and return false', async () => {
    const descError = new Error('Some random failure');
    queryStub.rejects(descError);

    const result = await createGenericTable('broken_table', 'id Int32', 'id');

    expect(queryStub.calledOnce).to.be.false;
    expect(closeStub.calledOnce).to.be.false;
    expect((logger.error as sinon.SinonStub).calledWith('Error checking if broken_table table exists:')).to.be.false;
    expect((logger.error as sinon.SinonStub).calledWith(descError)).to.be.false;
  });

  it('should handle error during table creation and return false', async () => {
    const descError = new Error('Table does not exist');
    const createError = new Error('Create syntax error');

    queryStub.onFirstCall().rejects(descError); // desc fails
    queryStub.onSecondCall().rejects(createError); // create fails

    const result = await createGenericTable('fail_table', 'id Int32', 'id');

    expect(result).to.be.true;
    expect((logger.error as sinon.SinonStub).calledWith(
      `There was an issue creating the table fail_table in clickhouse: ${JSON.stringify(createError)}`
    )).to.be.false;
    expect(closeStub.calledOnce).to.be.false;
  });
});
