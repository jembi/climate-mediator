import chai from "chai";
import nock from "nock";
import { config } from "../../src/config/config";

export const mockConfig = {
  ...config,
  // Add any specific test configurations here
};

export const setupTestEnvironment = (): void => {
  // Disable real HTTP requests during tests
  nock.disableNetConnect();
  // Allow localhost connections for local testing
  nock.enableNetConnect("127.0.0.1");
};

export const teardownTestEnvironment = (): void => {
  // Clean up all nock interceptors
  nock.cleanAll();
  // Re-enable real HTTP requests
  nock.enableNetConnect();
};

// Example of how to use nock to mock HTTP requests
export const mockOpenHIMRequest = (): nock.Scope => {
  return nock("https://localhost:8080")
    .post("/mediators")
    .reply(201, { success: true });
}; 