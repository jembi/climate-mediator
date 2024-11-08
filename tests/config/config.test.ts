import { expect } from "chai";
import { Config } from "../../src/config/config";

describe("Config", () => {
  const OLD_ENV = process.env;

  beforeEach(() => {
    process.env = { ...OLD_ENV };
  });

  afterEach(() => {
    process.env = OLD_ENV;
  });

  it("should load default values when no env variables are set", () => {
    const { config } = require("../../src/config/config");
    
    expect(config.port).to.equal(3000);
    expect(config.logLevel).to.equal("debug");
    expect(config.registerMediator).to.equal(true);
    expect(config.openhimMediatorUrl).to.equal("https://localhost:8080");
    expect(config.openhimUsername).to.equal("root@openhim.org");
    expect(config.openhimPassword).to.equal("instant101");
    expect(config.trustSelfSigned).to.equal(true);
    expect(config.runningMode).to.equal("");
    expect(config.bodySizeLimit).to.equal("50mb");
  });

  it("should use environment variables when provided", () => {
    // Clear the config module from require cache
    delete require.cache[require.resolve('../../src/config/config')];
    
    process.env.SERVER_PORT = "3000";
    process.env.LOG_LEVEL = "info";
    process.env.REGISTER_MEDIATOR = "false";
    process.env.OPENHIM_MEDIATOR_URL = "https://test.com";
    process.env.OPENHIM_USERNAME = "test@test.com";
    process.env.OPENHIM_PASSWORD = "test123";
    process.env.TRUST_SELF_SIGNED = "false";
    process.env.MODE = "test";
    process.env.BODY_SIZE_LIMIT = "100mb";

    // Now reload config
    const { config } = require("../../src/config/config");
    
    expect(config.port).to.equal(3000);
    expect(config.logLevel).to.equal("info");
    expect(config.registerMediator).to.equal(false);
    expect(config.openhimMediatorUrl).to.equal("https://test.com");
    expect(config.openhimUsername).to.equal("test@test.com");
    expect(config.openhimPassword).to.equal("test123");
    expect(config.trustSelfSigned).to.equal(false);
    expect(config.runningMode).to.equal("test");
    expect(config.bodySizeLimit).to.equal("100mb");
  });
}); 