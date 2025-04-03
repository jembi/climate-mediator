import chai from 'chai';
import nock from 'nock';
import sinon from 'sinon';
import winston from 'winston';
import { ModelPredictionUsingChap } from '../../src/services/ModelPredictionUsingChap';

const { expect } = chai;

describe('ModelPredictionUsingChap', () => {
  let modelPrediction: ModelPredictionUsingChap;
  let logger: winston.Logger;
  const chapUrl = 'https://fake-chap-url.com';

  beforeEach(() => {
    logger = winston.createLogger({
      transports: [new winston.transports.Console()],
    });

    sinon.stub(logger, 'info');
    sinon.stub(logger, 'error');

    modelPrediction = new ModelPredictionUsingChap(chapUrl, logger);
  });

  afterEach(() => {
    nock.cleanAll();
  });

  describe('predict', () => {
    it('should make a prediction request and return response', async () => {
      const mockResponse = { prediction: 'result' };
      const request = { data: 'some data' };

      nock(chapUrl).post('/predict').reply(200, mockResponse);

      const result = await modelPrediction.predict(request);

      expect(result).to.deep.equal(mockResponse);
    });

    it('should handle errors gracefully', async () => {
      const request = { data: 'some data' };

      nock(chapUrl).post('/predict').replyWithError('Error during prediction');

      try {
        await modelPrediction.predict(request);
      } catch (err: any) {
        expect(err.message).to.equal('Error during prediction');
      }
    });
  });

  describe('getStatus', () => {
    it('should fetch the status of the prediction', async () => {
      const mockResponse = { status: 'completed' };

      nock(chapUrl).get('/status').reply(200, mockResponse);

      const result = await modelPrediction.getStatus();

      expect(result).to.deep.equal(mockResponse);
    });

    it('should handle errors gracefully', async () => {
      nock(chapUrl).get('/status').replyWithError('Error fetching status');

      try {
        await modelPrediction.getStatus();
      } catch (err: any) {
        expect(err.message).to.equal('Error fetching status');
      }
    });
  });

  describe('getResult', () => {
    it('should fetch the result of the prediction', async () => {
      const mockResponse = { result: 'final result' };

      nock(chapUrl).get('/get-results').reply(200, mockResponse);

      const result = await modelPrediction.getResult();

      expect(result).to.deep.equal({ data: mockResponse });
    });

    it('should handle errors gracefully', async () => {
      nock(chapUrl).get('/get-results').replyWithError('Error fetching results');

      try {
        await modelPrediction.getResult();
      } catch (err: any) {
        expect(err.message).to.equal('Error fetching results');
      }
    });
  });
});
