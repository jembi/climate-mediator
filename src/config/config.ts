import { LogLevel } from '../types/mediatorConfig';

export interface Config {
  port: number;
  logLevel: LogLevel;
  registerMediator: boolean;
  openhimMediatorUrl: string;
  openhimUsername: string;
  openhimPassword: string;
  trustSelfSigned: boolean;
  runningMode: string;
  bodySizeLimit: string;
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
    minio: {
      endPoint: process.env.MINIO_ENDPOINT || "localhost",
      port: process.env.MINIO_PORT ? parseInt(process.env.MINIO_PORT) : 9000,
      useSSL: process.env.MINIO_USE_SSL === "false" ? false : true,
      bucket: process.env.MINIO_BUCKET || "climate-mediator",
      accessKey: process.env.MINIO_ACCESS_KEY || "tCroZpZ3usDUcvPM3QT6",
      secretKey: process.env.MINIO_SECRET_KEY || "suVjMHUpVIGyWx8fFJHTiZiT88dHhKgVpzvYTOKK",
      prefix: process.env.MINIO_PREFIX || "",
      suffix: process.env.MINIO_SUFFIX || "",
    },
  })
}
