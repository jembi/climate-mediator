enum LogLevel {
  debug = "debug",
  info = "info",
  warn = "warn",
  error = "error",
  silent = "silent",
}

export const getConfig = () => {
  return Object.freeze({
    port: process.env.SERVER_PORT || 3000,
    logLevel: (process.env.LOG_LEVEL || "debug") as LogLevel,
    registerMediator: process.env.REGISTER_MEDIATOR === "false" ? false : true,
    openhimMediatorUrl:
      process.env.OPENHIM_MEDIATOR_URL || "https://localhost:8080",
    openhimUsername: process.env.OPENHIM_USERNAME || "root@openhim.org",
    openhimPassword: process.env.OPENHIM_PASSWORD || "instant101",
    trustSelfSigned: process.env.TRUST_SELF_SIGNED === "false" ? false : true,
    runningMode: process.env.MODE || "",
    bodySizeLimit: process.env.BODY_SIZE_LIMIT || "50mb",
  });
};
