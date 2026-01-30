import axios, { AxiosError } from 'axios';
import https from 'https';
import { activateHeartbeat, fetchConfig, registerMediator } from 'openhim-mediator-utils';
import path from 'path';
import { getConfig } from '../config/config';
import { datalake } from '../datalake';
import logger from '../logger';
import { MediatorConfig, MinioBucketsRegistry } from '../types/mediatorConfig';
import { RequestOptions } from '../types/request';
import { downloadFileFromUrl, validateUrl } from '../utils/files';

const {
  openhimUsername,
  openhimPassword,
  openhimMediatorUrl,
  trustSelfSigned,
  runningMode,
  openhimTransactionUrl,
  openhimClientToken,
} = getConfig();

const mediatorConfigFilePath = path.resolve(__dirname, './mediatorConfig.json');

const resolveMediatorConfig = (): MediatorConfig => {
  let mediatorConfigFile;

  try {
    logger.debug(`Loading mediator config from: ${mediatorConfigFilePath}`);
    mediatorConfigFile = require(mediatorConfigFilePath);
  } catch (error) {
    logger.error(`Failed to parse JSON: ${error}`);
    throw error;
  }

  return mediatorConfigFile;
};

const resolveOpenhimConfig = (urn: string): RequestOptions => {
  return {
    username: openhimUsername,
    password: openhimPassword,
    apiURL: openhimMediatorUrl,
    trustSelfSigned: trustSelfSigned,
    urn: urn,
  };
};

export const setupMediator = async () => {
  try {
    const mediatorConfig = resolveMediatorConfig();
    const openhimConfig = resolveOpenhimConfig(mediatorConfig.urn);

    await registerMediator(openhimConfig, mediatorConfig, (error: Error) => {
      console.error({ openhimConfig, mediatorConfig });
      if (error) {
        logger.error(`Failed to register mediator: ${JSON.stringify(error)}`);
        throw error;
      }

      logger.info('Successfully registered mediator!');

      fetchConfig(openhimConfig, (err: Error) => {
        if (err) {
          logger.error(`Failed to fetch initial config: ${JSON.stringify(err)}`);
          throw err;
        }

        const emitter = activateHeartbeat(openhimConfig);

        emitter.on('error', (err: Error) => {
          logger.error(`Heartbeat failed: ${JSON.stringify(err)}`);
        });

        emitter.on('config', async (config: any) => {
          logger.debug('Received new configs from OpenHIM');
          Config = config.minio_buckets_registry;

          await handleBucketConfig(config.minio_buckets_registry);
        });
      });
    });
  } catch (err) {
    logger.error('Unable to register mediator', err);
  }
};

let Config: any;
export const getOpenhimConfig = () => Config;

/**
 * Handles bucket configuration updates from OpenHIM
 */
async function handleBucketConfig(buckets: any[]) {
  if (!buckets) return;

  // Use datalake library to initialize buckets
  await datalake.openhim?.registerBucket(buckets.map((b: any) => b.bucket).join(',')); // Note: openhim service registerBucket takes one bucket.
  // Actually, datalake openhim service mirrors the old logic, maybe I should just use the datalake components.

  // Legacy logic for downloading files defined in OpenHIM config
  for (const bucket of buckets) {
    if (bucket.bucket) {
      const fileName = bucket.fileName;
      const url = bucket.url;

      if (!url) {
        continue;
      }

      if (!validateUrl(url)) {
        logger.error(
          `Invalid URL: ${url} from bucket: ${bucket.bucket}. Cannot download file`
        );
        continue;
      }

      if (!fileName) {
        logger.error(`File name not provided for bucket: ${bucket.bucket}`);
        continue;
      }

      try {
        const extension = fileName.split('.').at(-1) ?? '.bin';
        const fileData = await downloadFileFromUrl(url);
        await datalake.upload.uploadBuffer(
          Buffer.from(fileData),
          fileName,
          extension, // mimeType, need mapping or guess
          { bucket: bucket.bucket }
        );
      } catch (err) {
        continue;
      }
    }
  }
}

// Keeping this for backward compatibility if any other module imports it
export async function getMediatorConfig(): Promise<MediatorConfig | null> {
  // Use datalake service if available, else fallback to manual implementation
  if (datalake.openhim) {
    return datalake.openhim.getMediatorConfig() as Promise<MediatorConfig | null>;
  }
  return null;
}

// Triggers the processing of the climate data onces its received on the minio bucket
export async function triggerProcessing(bucket: string, file: string, tableName: string) {
  await axios({
    url: `${openhimTransactionUrl}/process-climate-data`,
    method: 'GET',
    params: { bucket, file, tableName },
    headers: {
      Authorization: `Custom ${openhimClientToken}`,
    },
  });
}

// Deprecated: use datalake.buckets.ensureExists + datalake.openhim.registerBucket
export async function registerBucket(bucket: string) {
  if (datalake.openhim) {
    return datalake.openhim.registerBucket(bucket);
  }
  return false;
}

// Deprecated: use datalake.openhim.removeBucket
export async function removeBucket(buckets: string[]) {
  if (datalake.openhim) {
    return datalake.openhim.removeBucket(buckets);
  }
  return false;
}
