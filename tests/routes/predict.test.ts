import { expect } from 'chai';
import request from 'supertest';
import sinon, { SinonStub } from 'sinon';
import fs from 'fs';
import path from 'path';
import app from '../../src/index';
import * as minioClient from '../../src/utils/minioClient';
import logger from '../../src/logger';
import { ModelPredictionUsingChap } from '../../src/services/ModelPredictionUsingChap';
import axios from 'axios';

describe('POST /synthetic-predict', function () {
  this.timeout(5000);

  let uploadToMinioStub: sinon.SinonStub;
  let axiosPostStub: sinon.SinonStub;

  let ensureBucketExistsStub: SinonStub;
  let loggerErrorStub: SinonStub;
  let modelPredictStub: SinonStub;
  let modelGetStatusStub: SinonStub;
  let modelGetResultStub: SinonStub;

  beforeEach(() => {
    ensureBucketExistsStub = sinon.stub(minioClient, 'ensureBucketExists').resolves();
    uploadToMinioStub = sinon.stub(minioClient, 'uploadToMinio').resolves({
      success: true,
      message: 'Upload successful',
    });

    axiosPostStub = sinon.stub(axios, 'post').resolves({
      status: 200,
      data: { predictions: [] },
    });

    process.env.CHAP_URL = 'http://localhost:8000';
    loggerErrorStub = sinon.stub(logger, 'error');

    modelPredictStub = sinon.stub(ModelPredictionUsingChap.prototype, 'predict').resolves({
      status: 'success',
    });
    modelGetStatusStub = sinon.stub(ModelPredictionUsingChap.prototype, 'getStatus').resolves({
      status: 'idle',
      ready: true,
    });

    modelGetResultStub = sinon.stub(ModelPredictionUsingChap.prototype, 'getResult').resolves({
      data: {
        dataValues: [
          {
            ou: 'test_ou',
            pe: '202502',
            value: 46,
          },
        ],
        diseaseId: 'test-disease',
      },
    });
  });

  afterEach(() => {
    sinon.restore();
  });

  it('should return 400 when no file is uploaded', async () => {
    const res = await request(app).post('/predict').expect(400);

    console.log('Response Body:', res.body);

    expect(res.body).to.have.property('status', 'error');
    expect(res.body).to.have.property('message', 'No file uploaded');
    expect(res.body).to.have.property('code', 'FILE_MISSING');
  });

  it('should process valid CSV files and return 201', async () => {
    const sandbox = sinon.createSandbox();
    const trainingCsv = `date,cases,region\n2023-01-01,100,North`;
    const historicCsv = `date,cases,region\n2023-01-02,150,North`;
    const futureCsv = `date,cases,region\n2023-01-03,200,North`;
    sandbox.stub(fs.promises, 'writeFile').resolves();
    sandbox.stub(fs.promises, 'unlink').resolves();
    sandbox.stub(path, 'join').returns('/tmp/prediction-result.json');

    const res = await request(app)
      .post('/synthetic-predict')
      .query({
        bucket: 'valid-bucket',
        createBucketIfNotExists: 'true',
      })
      .attach('training', Buffer.from(trainingCsv), 'training.csv')
      .attach('historic', Buffer.from(historicCsv), 'historic.csv')
      .attach('future', Buffer.from(futureCsv), 'future.csv');

    if (res.status !== 201) {
      console.error('Test failed:', {
        status: res.status,
        body: res.body,
      });
    }

    expect(res.status).to.equal(201);
    expect(res.body.status).to.equal('success');
    sinon.assert.calledWith(ensureBucketExistsStub, 'valid-bucket', true);
    sinon.assert.callCount(uploadToMinioStub, 4);
    sinon.assert.calledTwice(axiosPostStub);
    sinon.assert.calledWithMatch(
      uploadToMinioStub,
      sinon.match.string,
      'prediction-result.json',
      'valid-bucket',
      'application/json'
    );
  });
});
