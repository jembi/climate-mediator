import request from 'supertest';
import chai from 'chai';
import sinon from 'sinon';
import app from '../../src/index';
import * as minio from '../../src/utils/minioClient';

const { expect } = chai;

describe('GET /process-climate-data', () => {
  const testParams = {
    bucket: 'climate-data',
    file: 'payload.json',
    tableName: 'monthly_metrics',
  };

  let minioStub: sinon.SinonStub;

  beforeEach(() => {
    minioStub = sinon.stub(minio, 'minioListenerHandler').resolves();
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('Successful Processing', () => {
    it('should return a success response with processed data', async () => {
      const res = await request(app).get('/process-climate-data').query(testParams);

      expect(res.status).to.equal(200);
      expect(res.body).to.deep.equal({
        bucket: testParams.bucket,
        file: testParams.file,
        tableName: testParams.tableName,
        clickhouseInsert: 'Success',
      });
      expect(
        minioStub.calledOnceWithExactly(
          testParams.bucket,
          testParams.file,
          testParams.tableName
        )
      ).to.be.true;
    });
  });

  describe('Error Handling', () => {
    it('should return 500 if minioListenerHandler fails', async () => {
      minioStub.rejects(new Error('MinIO processing error'));

      const res = await request(app).get('/process-climate-data').query(testParams);

      expect(res.status).to.equal(500);
      expect(res.body).to.have.property('message', 'MinIO processing error');
    });
  });
});
