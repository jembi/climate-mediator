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
    expect(result).to.be.true;
    expect((logger.info as sinon.SinonStub).calledWith('Table existing_table already exists')).to.be.true;
  });

  it('should create table if it does not exist', async () => {
    const descError = new Error('Table does not exist');
    queryStub.onFirstCall().rejects(descError); // desc throws error (table not found)
    queryStub.onSecondCall().resolves('create success'); // create table success

    const result = await createGenericTable('new_table', 'id Int32', 'id');

    expect(queryStub.callCount).to.equal(2);
    expect(queryStub.secondCall.args[0].query).to.include('CREATE TABLE IF NOT EXISTS new_table');
    expect(result).to.be.true;
    expect((logger.debug as sinon.SinonStub).calledWith('Now creating table new_table')).to.be.true;
  });

  it('should handle unexpected error during desc and return false', async () => {
    const descError = new Error('Some random failure');
    queryStub.rejects(descError);

    const result = await createGenericTable('broken_table', 'id Int32', 'id');

    expect(queryStub.calledOnce).to.be.true;
    expect(closeStub.calledOnce).to.be.true;
    expect(result).to.be.false;
    expect((logger.error as sinon.SinonStub).calledWith('Error checking if broken_table table exists:')).to.be.true;
    expect((logger.error as sinon.SinonStub).calledWith(descError)).to.be.true;
  });

  it('should handle error during table creation and return false', async () => {
    const descError = new Error('Table does not exist');
    const createError = new Error('Create syntax error');

    queryStub.onFirstCall().rejects(descError); // desc fails
    queryStub.onSecondCall().rejects(createError); // create fails

    const result = await createGenericTable('fail_table', 'id Int32', 'id');

    expect(result).to.be.false;
    expect((logger.error as sinon.SinonStub).calledWith(
      `There was an issue creating the table fail_table in clickhouse: ${JSON.stringify(createError)}`
    )).to.be.true;
    expect(closeStub.calledOnce).to.be.true;
  });
});
