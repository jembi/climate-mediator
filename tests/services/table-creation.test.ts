import { expect } from 'chai';
import { getCsvHeaders } from '../../src/utils/file-validators';
import { flattenJson, generateDDL } from '../../src/utils/clickhouse';


describe('Create Tables based on files', function () {
  this.timeout(60_000);

  it('should extract columns based on a csv file', async () => {
    //arrange
    const csvFile = Buffer.from('id,name,age\n1,John,20\n2,Jane,21');
    //act
    const fields = getCsvHeaders(csvFile);
    //assert
    expect(fields).to.deep.equal(['id', 'name', 'age']);
  });

  it('should extract columns based on a json file', async () => {
    const jsonFile = Buffer.from('[{"id":1,"name":"John","age":20}]');
    const json = JSON.parse(jsonFile.toString());
    const fields = flattenJson(json[0]);
    expect(fields).to.deep.equal(['id', 'name', 'age']);
  });

  it('should create a table based on a csv file', async () => {
    const csvFile = Buffer.from('id,name,age\n1,John,20\n2,Jane,21');
    const fields = getCsvHeaders(csvFile);
    if (!fields) throw new Error('No fields found');
    const result = generateDDL(fields, 'test');
    expect(result).to.equal('CREATE TABLE test (table_id UUID DEFAULT generateUUIDv4(),id VARCHAR, name VARCHAR, age VARCHAR) ENGINE = MergeTree ORDER BY (table_id)');
  });
});
