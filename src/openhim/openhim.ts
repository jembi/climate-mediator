import logger from '../logger';
import { MediatorConfig, MinioBucketsRegistry } from '../types/mediatorConfig';
import { RequestOptions } from '../types/request';
import { getConfig } from '../config/config';
import axios, { AxiosError } from 'axios';
import https from 'https';
import { activateHeartbeat, fetchConfig, registerMediator } from 'openhim-mediator-utils';
import { Bucket, createMinioBucketListeners, ensureBucketExists } from '../utils/minioClient';
import path from 'path';
import { validateBucketName } from '../utils/file-validators';

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
          logger.debug('Received new configs from OpenHIM');
          const mediatorConfig = {
            config: {
              minio_buckets_registry: config.minio_buckets_registry,
            },
            defaultChannelConfig: [],
            endpoints: [],
            urn: config.urn,
            version: config.version,
            name: config.name,
            description: config.description,
          };
          await initializeBuckets(mediatorConfig);
        });
      });
    });
  } catch (err) {
    logger.error('Unable to register mediator', err);
  }
};

/**
 * Initializes the buckets based on the values in the mediator config
 * if the bucket is invalid, it will be removed from the config
 * otherwise, the bucket will be created if it doesn't exist
 * and the listeners will be created for the valid buckets
 *
 * @param mediatorConfig - The mediator config
 */
export async function initializeBuckets(mediatorConfig: MediatorConfig) {
  const bucketsFromOpenhimConfig = mediatorConfig.config?.minio_buckets_registry as Bucket[];
  const validBuckets: string[] = [];
  const invalidBuckets: string[] = [];

  for await (const { bucket, region } of bucketsFromOpenhimConfig) {
    if (!validateBucketName(bucket)) {
      logger.error(`Invalid bucket name ${bucket}, skipping`);
      invalidBuckets.push(bucket);
    } else {
      await ensureBucketExists(bucket, region, true);
      validBuckets.push(bucket);
    }
  }

  await createMinioBucketListeners(validBuckets);

  if (invalidBuckets.length > 0) {
    await removeBucket(invalidBuckets);
    logger.info(`Removed ${invalidBuckets.length} invalid buckets`);
  }
}

export async function getMediatorConfig(): Promise<MediatorConfig | null> {
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
  if (runningMode === 'testing') {
    logger.info('Running in testing mode, reading buckets from ENV');
    const buckets = getConfig().minio.buckets.split(',');
    return buckets.map((bucket) => ({ bucket, region: '' }));
  }

  logger.info('Fetching registered buckets from OpenHIM');
  const mediatorConfig = await getMediatorConfig();

  if (!mediatorConfig) {
    return [];
  }

  if (mediatorConfig) {
    await initializeBuckets(mediatorConfig);
    return mediatorConfig.config?.minio_buckets_registry as Bucket[];
  }
  return [];
}

export async function registerBucket(bucket: string, region?: string) {
  // If we are in testing mode, we don't need to have the registered buckets persisted
  if (runningMode === 'testing') {
    logger.debug('Running in testing mode, skipping bucket registration');
    return true;
  }

  //get the mediator config from OpenHIM
  const mediatorConfig = await getMediatorConfig();

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

export async function removeBucket(buckets: string[]) {
  const mediatorConfig = await getMediatorConfig();

  if (!mediatorConfig) {
    logger.error('Mediator config not found in OpenHIM, unable to remove bucket');
    return false;
  }

  const existingConfig = mediatorConfig.config;

  if (existingConfig === undefined) {
    logger.error('Mediator config does not have a config section, unable to remove bucket');
    return false;
  }

  const updatedConfig = existingConfig.minio_buckets_registry.filter(
    (b) => !buckets.includes(b.bucket)
  );

  await putMediatorConfig(mediatorConfig.urn, updatedConfig);

  return true;
}
