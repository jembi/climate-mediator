import logger from '../logger';
import { MediatorConfig } from '../types/mediatorConfig';
import { RequestOptions } from '../types/request';
import { getConfig } from '../config/config';
import axios, { AxiosError } from 'axios';
import https from 'https';
import {
  activateHeartbeat,
  fetchConfig,
  registerMediator,
  authenticate,
  genAuthHeaders,
} from 'openhim-mediator-utils';
import { Bucket, createMinioBucketListeners, ensureBucketExists } from '../utils/minioClient';
import path from 'path';

const { openhimUsername, openhimPassword, openhimMediatorUrl, trustSelfSigned, runningMode } =
  getConfig();

const mediatorConfigFilePath = path.resolve(__dirname, './mediatorConfig.json');

const resolveMediatorConfig = (): MediatorConfig => {
  let mediatorConfigFile;

  try {
    logger.info(`Loading mediator config from: ${mediatorConfigFilePath}`);
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

export const setupMediator = () => {
  try {
    const mediatorConfig = resolveMediatorConfig();
    const openhimConfig = resolveOpenhimConfig(mediatorConfig.urn);

    registerMediator(openhimConfig, mediatorConfig, (error: Error) => {
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
          logger.info('Received config from OpenHIM');

          const buckets = config.minio_buckets_registry as Bucket[];

          for await (const { bucket, region } of buckets) {
            await ensureBucketExists(bucket, region);
          }

          createMinioBucketListeners(buckets.map((bucket) => bucket.bucket));
        });
      });
    });
  } catch (err) {
    logger.error('Unable to register mediator', err);
  }
};

//TODO: Add Typing and error handling.
async function getMediatorConfig() {
  logger.info('Fetching mediator config from OpenHIM');
  const mediatorConfig = resolveMediatorConfig();
  const openhimConfig = resolveOpenhimConfig(mediatorConfig.urn);

  const { apiURL, urn, username, password, trustSelfSigned } = openhimConfig;

  try {
    const request = await axios.get(`${apiURL}/mediators/urn:mediator:climate-mediator`, {
      auth: {
        username,
        password,
      },
      httpsAgent: new https.Agent({
        rejectUnauthorized: !trustSelfSigned,
      }),
    });

    return request.data;
  } catch (e) {
    const error = e as AxiosError;
    logger.error(`Failed to fetch mediator config: ${JSON.stringify(error)}`);
    error.status === 404 && logger.warn('Mediator config not found in OpenHIM, ');
    return null;
  }
}

export async function getRegisterBuckets(): Promise<Bucket[]> {
  if (runningMode !== 'testing') {
    logger.info('Fetching registered buckets from OpenHIM');
    const mediatorConfig = await getMediatorConfig();

    //TODO: Handle errors, and undefined response.
    const buckets = mediatorConfig.config?.minio_buckets_registry as Bucket[];
    if (!buckets) {
      return [];
    }
    return buckets;
  } else {
    logger.info('Running in testing mode, reading buckets from ENV');
    const buckets = getConfig().minio.buckets.split(',');
    return buckets.map((bucket) => ({ bucket, region: '' }));
  }
}

export async function registerBucket(bucket: string, region?: string) {
  if (runningMode !== 'testing') {
    return true;
  }
  const mediatorConfig = await getMediatorConfig();
  const existingBuckets = mediatorConfig.config?.minio_buckets_registry;

  if (!existingBuckets) {
    return [];
  }
}
