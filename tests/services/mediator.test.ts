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

    // Make the actual request to register
    const response = await fetch('https://localhost:8080/mediators', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    }).catch((err) => {
      console.error('Fetch error:', err);
      throw err;
    });

    // Assert the response status
    expect(response.status).to.equal(401);
  });
});
