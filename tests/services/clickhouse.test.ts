import { expect } from 'chai';
import sinon from 'sinon';
import { createClient } from '@clickhouse/client';
import { createGenericTable } from '../../src/utils/clickhouse';
import { getConfig } from '../../src/config/config';
import logger from '../../src/logger';

describe('createGenericTable', () => {
  // Setup common test variables
  const mockClient = {
    query: sinon.stub(),
    close: sinon.stub().resolves()
  };
  
  let createClientStub: sinon.SinonStub;
  let configStub: sinon.SinonStub;
  let loggerStub: {
    info: sinon.SinonStub;
    debug: sinon.SinonStub;
    error: sinon.SinonStub;
  };
  
  beforeEach(() => {
    // Reset stubs before each test
    const { clickhouse } = getConfig();
    const { url, password } = clickhouse;
    createClientStub = sinon.stub(require('@clickhouse/client'), 'createClient').returns(mockClient);
    
    configStub = sinon.stub(require('../../src/config/config'), 'getConfig').returns({
      clickhouse: {
        url: url,
        password: password
      }
    });

    loggerStub = {
      info: sinon.stub(logger, 'info'),
      debug: sinon.stub(logger, 'debug'),
      error: sinon.stub(logger, 'error')
    };
    
    // Reset the stubs for each test
    mockClient.query.reset();
    mockClient.close.reset();
  });
  
  afterEach(() => {
    // Restore all stubs
    sinon.restore();
  });

  // it('should return false if table already exists', async () => {
  //   // Setup
  //   mockClient.query.resolves({ rows: [] }); // Table exists response
    
  //   // Execute
  //   const result = await createGenericTable(
  //     'test_table',
  //     'id String, name String',
  //     'id'
  //   );
    
  //   // Assert
  //   expect(result).to.be.false;
  //   expect(mockClient.query.calledWith({
  //     query: 'desc test_table',
  //   })).to.be.true;
  //   expect(mockClient.close.called).to.be.true;
  //   expect(loggerStub.info.calledWith('Table test_table already exists')).to.be.true;
  // });

  it('should create a table successfully if it does not exist', async () => {
    // Setup
    // First call should fail with table not found error
    const notFoundError = new Error('Table not found');
    mockClient.query.onFirstCall().rejects(notFoundError);
    // Second call (table creation) should succeed
    mockClient.query.onSecondCall().resolves({});
    
    // Execute
    const result = await createGenericTable(
      'test_table',
      'id String, name String',
      'id'
    );
    
    // Assert
    expect(result).to.be.true;
    expect(mockClient.query.calledTwice).to.be.true;
    expect(mockClient.query.firstCall.args[0]).to.deep.equal({
      query: 'desc test_table',
    });
    expect(mockClient.query.secondCall.args[0].query).to.include('CREATE TABLE IF NOT EXISTS test_table');
    expect(loggerStub.debug.calledWith('Table test_table does not exist')).to.be.true;
    expect(loggerStub.debug.calledWith('test_table table created successfully')).to.be.true;
  });

  it('should use custom engine when provided', async () => {
    // Setup
    // First call should fail with table not found error
    const notFoundError = new Error('Table does not exist');
    mockClient.query.onFirstCall().rejects(notFoundError);
    // Second call (table creation) should succeed
    mockClient.query.onSecondCall().resolves({});
    
    // Execute
    const result = await createGenericTable(
      'test_table',
      'id String, name String',
      'id',
      'ReplacingMergeTree'
    );
    
    // Assert
    expect(result).to.be.true;
    expect(mockClient.query.secondCall.args[0].query).to.include('ENGINE = ReplacingMergeTree()');
  });

  it('should handle errors when checking if table exists', async () => {
    // Setup
    const unexpectedError = new Error('Connection failed');
    // This error is not a "table not found" error
    mockClient.query.rejects(unexpectedError);
    
    // Execute
    const result = await createGenericTable(
      'test_table',
      'id String, name String',
      'id'
    );
    
    // Assert
    expect(result).to.be.false;
    expect(loggerStub.error.calledWith('Error checking if test_table table exists:')).to.be.true;
    expect(loggerStub.error.calledWith(unexpectedError)).to.be.true;
    expect(mockClient.close.called).to.be.true;
  });

  it('should handle errors when creating a table', async () => {
    // Setup
    // First call should fail with table not found error (expected)
    mockClient.query.onFirstCall().rejects(new Error('Table not found'));
    // Second call (table creation) should fail with an error
    const creationError = new Error('Failed to create table');
    mockClient.query.onSecondCall().rejects(creationError);
    
    // Execute
    const result = await createGenericTable(
      'test_table',
      'id String, name String',
      'id'
    );
    
    // Assert
    expect(result).to.be.false;
    expect(loggerStub.error.calledWith(sinon.match('There was an issue creating the table test_table in clickhouse:'))).to.be.true;
    expect(mockClient.close.called).to.be.true;
  });

  it('should always close the client connection, even when errors occur', async () => {
    // Setup
    mockClient.query.onFirstCall().rejects(new Error('Table not found')); // Table doesn't exist
    mockClient.query.onSecondCall().rejects(new Error('Failed to create table')); // Table creation fails
    
    // Execute
    await createGenericTable(
      'test_table',
      'id String, name String',
      'id'
    );
    
    // Assert
    expect(mockClient.close.called).to.be.true;
  });
});