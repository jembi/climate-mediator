import logger from '../logger';
import { MinioBucketsRegistry, Mediator as OpenHimAPIMediator } from '../types';
import { MediatorConfig } from '../types/mediatorConfig';
import { RequestOptions } from '../types/request';
import { getConfig } from '../config/config';
import axios, { AxiosError } from 'axios';
import https from 'https';
import { activateHeartbeat, fetchConfig, registerMediator } from 'openhim-mediator-utils';
import { Bucket, createMinioBucketListeners, ensureBucketExists } from '../utils/minioClient';
import path from 'path';

const { openhimUsername, openhimPassword, openhimMediatorUrl, trustSelfSigned, runningMode } =
  getConfig();

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
async function getMediatorConfig(): Promise<OpenHimAPIMediator | null> {
  logger.debug('Fetching mediator config from OpenHIM');
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

    switch (error.status) {
      case 401:
        logger.error(`Failed to authenticate with OpenHIM, check your credentials`);
        break;
      case 404:
        logger.debug(
          'Mediator config not found in OpenHIM, This is expected on initial setup'
        );
        break;
      default:
        logger.error(`Failed to fetch mediator config: ${JSON.stringify(error)}`);
        break;
    }
    return null;
  }
}

async function putMediatorConfig(mediatorUrn: string, mediatorConfig: MinioBucketsRegistry[]) {
  const openhimConfig = resolveOpenhimConfig(mediatorUrn);
  const { apiURL, username, password, trustSelfSigned } = openhimConfig;
  await axios.put(
    `${apiURL}/mediators/urn:mediator:climate-mediator/config`,
    {
      minio_buckets_registry: mediatorConfig,
    },
    {
      auth: { username, password },
      httpsAgent: new https.Agent({
        rejectUnauthorized: !trustSelfSigned,
      }),
    }
  );

  try {
    logger.info('Successfully updated mediator config in OpenHIM');
  } catch (error) {
    const axiosError = error as AxiosError;
    switch (axiosError.status) {
      case 401:
        logger.error(`Failed to authenticate with OpenHIM, check your credentials`);
        break;
      default:
        logger.error(
          `Failed to update mediator config in OpenHIM: ${JSON.stringify(axiosError)}`
        );
        break;
    }
  }
}

export async function getRegisteredBuckets(): Promise<Bucket[]> {
  if (runningMode !== 'testing') {
    logger.info('Fetching registered buckets from OpenHIM');
    const mediatorConfig = await getMediatorConfig();

    if (!mediatorConfig) {
      return [];
    }
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
  // If we are in testing mode, we don't need to have the registered buckets persisted
  if (runningMode === 'testing') {
    logger.debug('Running in testing mode, skipping bucket registration');
    return true;
  }

  //get the mediator config from OpenHIM
  const mediatorConfig = await getMediatorConfig();

  //TODO: Change this to a debug log
  logger.debug(`Mediator config: ${JSON.stringify(mediatorConfig)}`);

  //if the mediator config is not found, log the issue and return false
  if (mediatorConfig === null) {
    logger.error('Mediator config not found in OpenHIM, unable to register bucket');
    return false;
  }

  const newBucket = {
    bucket,
    region: region || '',
  };

  //get the existing buckets from the mediator config
  const existingConfig = mediatorConfig.config;

  if (existingConfig === undefined) {
    logger.info('Mediator config does not have a config section, creating new config');
    mediatorConfig['config'] = {
      minio_buckets_registry: [newBucket],
    };
  } else {
    const existingBucket = existingConfig.minio_buckets_registry.find(
      (bucket) => bucket.bucket === newBucket.bucket
    );
    if (existingBucket) {
      logger.debug(`Bucket ${bucket} already exists in the config`);
      return false;
    }
    logger.info(`Adding bucket ${bucket} to OpenHIM config`);
    existingConfig.minio_buckets_registry.push(newBucket);
    await putMediatorConfig(mediatorConfig.urn, existingConfig.minio_buckets_registry);
  }

  return true;
}
