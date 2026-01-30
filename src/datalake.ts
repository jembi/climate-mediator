/**
 * Datalake library initialization for climate-mediator
 *
 * This module initializes the @jembi/openhim-datalake-lib with climate-mediator's
 * configuration and registers climate-specific processors.
 */

import {
  createDatalakeLib,
  mediatorEvents,
  DatalakeLibConfig,
} from '@jembi/openhim-datalake-lib';
import { getConfig } from './config/config';
import logger from './logger';
import { ClimateDataProcessor } from './processors/climate';

const config = getConfig();

const datalakeConfig: DatalakeLibConfig = {
  datalake: {
    endPoint: config.minio.endPoint,
    port: config.minio.port,
    useSSL: config.minio.useSSL,
    accessKey: config.minio.accessKey,
    secretKey: config.minio.secretKey,
    region: config.minio.bucketRegion,
  },
  openhim:
    config.runningMode !== 'testing'
      ? {
          apiURL: config.openhimMediatorUrl,
          username: config.openhimUsername,
          password: config.openhimPassword,
          trustSelfSigned: config.trustSelfSigned,
          mediatorUrn: 'urn:mediator:climate-mediator',
        }
      : undefined,
  logger: {
    debug: (msg: string, ...args: any[]) => logger.debug(msg, ...args),
    info: (msg: string, ...args: any[]) => logger.info(msg, ...args),
    warn: (msg: string, ...args: any[]) => logger.warn(msg, ...args),
    error: (msg: string, ...args: any[]) => logger.error(msg, ...args),
  },
};

/**
 * Shared datalake library instance for the climate mediator
 */
export const datalake = createDatalakeLib(datalakeConfig);

// Register climate-specific file processor
datalake.listeners.registerProcessor(new ClimateDataProcessor());

// Log events from other mediators (for debugging/monitoring)
mediatorEvents.onUpload((event: { source?: string; file: string; bucket: string }) => {
  if (event.source && event.source !== 'climate-mediator') {
    logger.debug(`File ${event.file} uploaded by ${event.source} to ${event.bucket}`);
  }
});

export { mediatorEvents };
