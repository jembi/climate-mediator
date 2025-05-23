import axios, { AxiosError } from 'axios';
import https from 'https';
import { activateHeartbeat, fetchConfig, registerMediator } from 'openhim-mediator-utils';
import path from 'path';
import { Config, getConfig } from '../config/config';
import logger from '../logger';
import { MediatorConfig, MinioBucketsRegistry } from '../types/mediatorConfig';
import { RequestOptions } from '../types/request';
import { validateBucketName } from '../utils/file-validators';
import { downloadFileFromUrl, validateUrl } from '../utils/files';
import {
  createMinioBucketListeners,
  ensureBucketExists,
  uploadFileBufferToMinio,
} from '../utils/minioClient';

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
          await initializeBuckets(config.minio_buckets_registry);

          for (const bucket of config.minio_buckets_registry) {
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
                await uploadFileBufferToMinio(
                  Buffer.from(fileData),
                  fileName,
                  bucket.bucket,
                  extension
                );
              } catch (err) {
                continue;
              }
            }
          }
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
 * Initializes the buckets based on the values in the mediator config
 * if the bucket is invalid, it will be removed from the config
 * otherwise, the bucket will be created if it doesn't exist
 * and the listeners will be created for the valid buckets
 *
 * @param mediatorConfig - The mediator config
 */
export async function initializeBuckets(buckets: MinioBucketsRegistry[]) {
  if (!buckets) {
    logger.error('No buckets found in mediator config');
    return;
  }

  const validBuckets: string[] = [];
  const invalidBuckets: string[] = [];

  for await (const { bucket, region } of buckets) {
    if (!validateBucketName(bucket)) {
      logger.error(`Invalid bucket name ${bucket}, skipping`);
      invalidBuckets.push(bucket);
    } else {
      await ensureBucketExists(bucket, true);
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

export async function registerBucket(bucket: string) {
  // If we are in testing mode, we don't need to have the registered buckets persisted
  if (runningMode === 'testing') {
    logger.debug('Running in testing mode, skipping bucket registration');
    return true;
  }

  //get the mediator config from OpenHIM
  const mediatorConfig = await getMediatorConfig();

  //if the mediator config is not found, log the issue and return false
  if (mediatorConfig === null) {
    logger.error('Mediator config not found in OpenHIM, unable to register bucket');
    return false;
  }

  const newBucket = {
    bucket,
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
