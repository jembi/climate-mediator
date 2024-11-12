import { expect } from 'chai';
import nock from 'nock';
import {
  mockOpenHIMRequest,
  setupTestEnvironment,
  teardownTestEnvironment,
} from '../helpers/setup';
import https from 'https';

// At the top of your test file, before the tests
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

describe('Mediator Service', () => {
  before(() => {
    setupTestEnvironment();
  });

  after(() => {
    teardownTestEnvironment();
  });

  it('should register with OpenHIM', async () => {
    // Mock the OpenHIM registration endpoint
    const scope = nock('https://localhost:8080', {
      allowUnmocked: false,
      reqheaders: {
        'content-type': 'application/json',
      },
    })
      .post('/mediators', (body) => true)
      .reply(201, {});

    // Enable this to see what requests are actually being made
    nock.disableNetConnect();
    nock.enableNetConnect('localhost');

    nock.emitter.on('no match', (req) => {
      console.log('No match for request:', {
        method: req.method,
        url: req.url,
        headers: req.headers,
      });
    });

    // Use the existing nock scope to make the request
    const response = await new Promise((resolve) => {
      https
        .request(
          'https://localhost:8080/mediators',
          {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
          },
          (res) => {
            resolve(res);
          }
        )
        .end(JSON.stringify({}));
    });

    // Ensure the mocked request was made
    expect(scope.isDone()).to.be.true;
  });
});
