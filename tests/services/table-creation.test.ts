import { expect } from 'chai';
import { getCsvHeaders } from '../../src/utils/file-validators';
import { createTable, generateDDL } from '../../src/utils/clickhouse';

describe('Create Tables based on files', function () {
  // this.timeout(60_000);

  it('should extract columns based on a csv file (linux - \\n)', async () => {
    //arrange
    const csvFile = Buffer.from('id,name,age\n1,John,20\n2,Jane,21');
    //act
    const fields = getCsvHeaders(csvFile);
    //assert
    expect(fields).to.deep.equal(['id', 'name', 'age']);
  });

  it('should extract columns based on a csv file (windows - \\r\\n)', async () => {
    const csvFile = Buffer.from('id,name,age\r\n1,John,20\r\n2,Jane,21');
    const fields = getCsvHeaders(csvFile);
    expect(fields).to.deep.equal(['id', 'name', 'age']);
  });

  it('should generate a table create ddl based on a csv file', async () => {
    const csvFile = Buffer.from('id,name,age\n1,John,20\n2,Jane,21');
    const fields = getCsvHeaders(csvFile);
    if (!fields) throw new Error('No fields found');
    const result = generateDDL(fields, 'test');
    expect(result).to.equal(
      'CREATE TABLE test (table_id UUID DEFAULT generateUUIDv4(),id VARCHAR, name VARCHAR, age VARCHAR) ENGINE = MergeTree ORDER BY (table_id)'
    );
  });

  it('should create a table based on a csv file', async () => {
    const csvFile = Buffer.from('id,name,age\n1,John,20\n2,Jane,21');
    const fields = getCsvHeaders(csvFile);
    if (!fields) throw new Error('No fields found');
    const result = await createTable(fields, 'test');
    expect(result).to.be.true;
  });

  it('should fail to create a table based on an invalid csv file', async () => {
    const csvFile = Buffer.from('');
    const fields = getCsvHeaders(csvFile) as any;
    expect(() => createTable(fields, 'test')).to.throw;
  });
});
