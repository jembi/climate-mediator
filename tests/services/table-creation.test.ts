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
});
