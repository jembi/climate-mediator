import { config, expect } from 'chai';
import { Config } from '../../src/config/config';

describe('Config', function () {
  const OLD_ENV = process.env;

  beforeEach(() => {
    process.env = { ...OLD_ENV };
  });

  afterEach(() => {
    process.env = OLD_ENV;
  });

  it('should load default values when no env variables are set', () => {
    const { getConfig } = require('../../src/config/config');

    expect(getConfig().port).to.equal(3000);
    expect(getConfig().logLevel).to.equal('debug');
    expect(getConfig().registerMediator).to.equal(true);
    expect(getConfig().openhimMediatorUrl).to.equal('https://localhost:8080');
    expect(getConfig().openhimUsername).to.equal('root@openhim.org');
    expect(getConfig().openhimPassword).to.equal('instant101');
    expect(getConfig().trustSelfSigned).to.equal(true);
    expect(getConfig().runningMode).to.equal('testing');
    expect(getConfig().bodySizeLimit).to.equal('50mb');
  });

  it('should use environment variables when provided', () => {
    // Clear the config module from require cache
    delete require.cache[require.resolve('../../src/config/config')];

    process.env.SERVER_PORT = '3000';
    process.env.LOG_LEVEL = 'info';
    process.env.REGISTER_MEDIATOR = 'false';
    process.env.OPENHIM_MEDIATOR_URL = 'https://test.com';
    process.env.OPENHIM_USERNAME = 'test@test.com';
    process.env.OPENHIM_PASSWORD = 'test123';
    process.env.TRUST_SELF_SIGNED = 'false';
    process.env.MODE = 'test';
    process.env.BODY_SIZE_LIMIT = '100mb';

    // Now reload config
    const { getConfig } = require('../../src/config/config');

    expect(getConfig().port).to.equal(3000);
    expect(getConfig().logLevel).to.equal('info');
    expect(getConfig().registerMediator).to.equal(false);
    expect(getConfig().openhimMediatorUrl).to.equal('https://test.com');
    expect(getConfig().openhimUsername).to.equal('test@test.com');
    expect(getConfig().openhimPassword).to.equal('test123');
    expect(getConfig().trustSelfSigned).to.equal(false);
    expect(getConfig().runningMode).to.equal('test');
    expect(getConfig().bodySizeLimit).to.equal('100mb');
  });
});
