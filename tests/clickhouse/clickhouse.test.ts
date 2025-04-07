const { describe, it, beforeEach, afterEach } = require('mocha');
const { expect } = require('chai');
const sinon = require('sinon');
const { ClickHouseClient } = require('../src/clickhouse'); // Adjust path as needed

describe('ClickHouse Functions', () => {
  let clickhouseClient;
  let queryStub;

  beforeEach(() => {
    // Setup test environment
    clickhouseClient = new ClickHouseClient();
    queryStub = sinon.stub(clickhouseClient, 'query');
  });

  afterEach(() => {
    // Clean up after each test
    sinon.restore();
  });

  describe('insertData', () => {
    it('should insert data successfully', async () => {
      // Arrange
      const testData = { id: 1, name: 'Test' };
      queryStub.resolves({ rows: 1 });

      // Act
      const result = await clickhouseClient.insertData('test_table', testData);

      // Assert
      expect(result.rows).to.equal(1);
      expect(queryStub.calledOnce).to.be.true;
      expect(queryStub.firstCall.args[0]).to.include('INSERT INTO test_table');
    });

    it('should handle insertion errors', async () => {
      // Arrange
      const testData = { id: 1, name: 'Test' };
      queryStub.rejects(new Error('Database error'));

      // Act & Assert
      try {
        await clickhouseClient.insertData('test_table', testData);
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error.message).to.equal('Database error');
      }
    });
  });

  describe('queryData', () => {
    it('should retrieve data successfully', async () => {
      // Arrange
      const expectedData = [{ id: 1, name: 'Test' }];
      queryStub.resolves({ rows: expectedData });

      // Act
      const result = await clickhouseClient.queryData('SELECT * FROM test_table');

      // Assert
      expect(result.rows).to.deep.equal(expectedData);
      expect(queryStub.calledOnce).to.be.true;
      expect(queryStub.firstCall.args[0]).to.equal('SELECT * FROM test_table');
    });

    it('should handle query errors', async () => {
      // Arrange
      queryStub.rejects(new Error('Query error'));

      // Act & Assert
      try {
        await clickhouseClient.queryData('SELECT * FROM test_table');
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error.message).to.equal('Query error');
      }
    });
  });

  describe('deleteData', () => {
    it('should delete data successfully', async () => {
      // Arrange
      const condition = "id = 1";
      queryStub.resolves({ rows: 1 });

      // Act
      const result = await clickhouseClient.deleteData('test_table', condition);

      // Assert
      expect(result.rows).to.equal(1);
      expect(queryStub.calledOnce).to.be.true;
      expect(queryStub.firstCall.args[0]).to.include('ALTER TABLE test_table DELETE WHERE');
      expect(queryStub.firstCall.args[0]).to.include(condition);
    });

    it('should handle deletion errors', async () => {
      // Arrange
      const condition = "id = 1";
      queryStub.rejects(new Error('Deletion error'));

      // Act & Assert
      try {
        await clickhouseClient.deleteData('test_table', condition);
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error.message).to.equal('Deletion error');
      }
    });
  });

  describe('createTable', () => {
    it('should create table successfully', async () => {
      // Arrange
      const tableName = 'new_test_table';
      const schema = 'id UInt32, name String, created_at DateTime';
      queryStub.resolves({ rows: 0 });

      // Act
      const result = await clickhouseClient.createTable(tableName, schema);

      // Assert
      expect(result.rows).to.equal(0);
      expect(queryStub.calledOnce).to.be.true;
      expect(queryStub.firstCall.args[0]).to.include(`CREATE TABLE IF NOT EXISTS ${tableName}`);
      expect(queryStub.firstCall.args[0]).to.include(schema);
    });

    it('should handle table creation errors', async () => {
      // Arrange
      const tableName = 'new_test_table';
      const schema = 'id UInt32, name String, created_at DateTime';
      queryStub.rejects(new Error('Table creation error'));

      // Act & Assert
      try {
        await clickhouseClient.createTable(tableName, schema);
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error.message).to.equal('Table creation error');
      }
    });
  });
});