import crypto from 'crypto';
import { readFile, rm } from 'fs/promises';
import * as Minio from 'minio';
import { getConfig } from '../config/config';
import logger from '../logger';
import {
  createOrganizationsTable,
  createTable,
  createTableFromJson,
  insertFromS3,
  insertFromS3Json,
  insertOrganizationIntoTable,
} from './clickhouse';
import { getCsvHeaders, validateJsonFile } from './file-validators';

export interface Bucket {
  bucket: string;
  region?: string;
}

export class BucketDoesNotExistError extends Error {
  constructor(message: string) {
    super(message);
  }
}

const { endPoint, port, useSSL, bucketRegion, accessKey, secretKey, buckets, prefix, suffix } =
  getConfig().minio;

const registeredBuckets: Set<string> = new Set();

// Create a shared Minio client instance
const minioClient = new Minio.Client({
  endPoint,
  port,
  useSSL,
  accessKey,
  secretKey,
});

interface MinioResponse {
  success: boolean;
  message: string;
}

interface FileExistsResponse extends MinioResponse {
  exists: boolean;
}

/**
 * Get the first field of a json object
 * @param {any} json - The json object
 * @returns {string} - The first field
 */
function getFirstField(json: any) {
  let obj: any;
  if (Array.isArray(json) && json.length > 0) {
    obj = json[0];
  } else {
    obj = json;
  }

  const fields = Object.keys(obj);
  return fields[0];
}

/**
 * Ensures a bucket exists, creates it if it doesn't
 * @param {string} bucket - Bucket name
 * @param {string} [region] - Bucket region
 * @param {boolean} [createBucketIfNotExists] - Whether to create the bucket if it doesn't exist
 * @returns {Promise<void>}
 */
export async function ensureBucketExists(
  bucket: string,
  region?: string,
  createBucketIfNotExists = false
): Promise<void> {
  try {
    const exists = await minioClient.bucketExists(bucket);
    if (!exists && createBucketIfNotExists) {
      await minioClient.makeBucket(bucket, region);
      logger.info(
        `Bucket ${bucket} created${region ? `in "${region}"` : ' no region specified'}`
      );
    }

    await createMinioBucketListeners([bucket]);

    if (!exists && !createBucketIfNotExists) {
      throw new BucketDoesNotExistError(`Bucket ${bucket} does not exist`);
    }
  } catch (error) {
    logger.error(`Error ensuring bucket ${bucket} exists: ${error}`);
    throw error;
  }
}

/**
 * Checks if a file exists in the specified Minio bucket
 * @param {string} fileName - Name of the file to check
 * @param {string} bucket - Bucket name
 * @param {string} fileType - Expected file type
 * @returns {Promise<FileExistsResponse>}
 */
export async function checkFileExists(
  fileName: string,
  bucket: string,
  fileType: string
): Promise<FileExistsResponse> {
  try {
    const bucketExists = await minioClient.bucketExists(bucket);
    if (!bucketExists) {
      return {
        exists: false,
        success: false,
        message: `Bucket ${bucket} does not exist`,
      };
    }

    const stats = await minioClient.statObject(bucket, fileName);
    const exists = stats.metaData?.['content-type'] === fileType;

    return {
      exists,
      success: true,
      message: exists
        ? `File ${fileName} exists in bucket ${bucket}`
        : `File ${fileName} does not exist in bucket ${bucket}`,
    };
  } catch (err) {
    const error = err as Error;
    if ((error as any).code === 'NotFound') {
      return {
        exists: false,
        success: true,
        message: `File ${fileName} not found in bucket ${bucket}`,
      };
    }

    logger.error('Error checking file existence:', error);
    return {
      exists: false,
      success: false,
      message: `Error checking file existence: ${error.message}`,
    };
  }
}

/**
 * Uploads a file to Minio storage
 * @param {string} sourceFile - Path to the file to upload
 * @param {string} destinationObject - Name for the uploaded object
 * @param {string} bucket - Bucket name
 * @param {string} fileType - Type of file being uploaded
 * @param {Object} [customMetadata={}] - Optional custom metadata
 * @returns {Promise<MinioResponse>}
 */
export async function uploadToMinio(
  sourceFile: string,
  destinationObject: string,
  bucket: string,
  fileType: string,
  customMetadata = {}
): Promise<MinioResponse> {
  try {
    logger.info(`Uploading file ${sourceFile} to bucket ${bucket}`);

    const fileCheck = await checkFileExists(destinationObject, bucket, fileType);

    if (fileCheck.exists) {
      return {
        success: false,
        message: fileCheck.message,
      };
    }

    const metaData = {
      'Content-Type': fileType,
      'X-Upload-Id': crypto.randomUUID(),
      ...customMetadata,
    };

    await minioClient.fPutObject(bucket, destinationObject, sourceFile, metaData);
    const successMessage = `File ${sourceFile} uploaded as object ${destinationObject} in bucket ${bucket}`;
    logger.info(successMessage);

    return {
      success: true,
      message: successMessage,
    };
  } catch (error) {
    const errorMessage = `Error uploading file: ${error instanceof Error ? error.message : String(error)}`;
    logger.error(errorMessage);
    return {
      success: false,
      message: errorMessage,
    };
  }
}

/**
 * Uploads a file buffer to Minio storage
 * @param {Buffer} fileBuffer - Path to the file to upload
 * @param {string} destinationObject - Name for the uploaded object
 * @param {string} bucket - Bucket name
 * @param {string} fileType - Type of file being uploaded
 * @param {Object} [customMetadata={}] - Optional custom metadata
 * @returns {Promise<MinioResponse>}
 */
export async function uploadFileBufferToMinio(
  fileBuffer: Buffer,
  destinationObject: string,
  bucket: string,
  fileType: string,
  customMetadata = {}
): Promise<MinioResponse> {
  try {
    logger.info(`Uploading file buffer ${customMetadata} to bucket ${bucket}`);

    const fileCheck = await checkFileExists(destinationObject, bucket, fileType);

    if (fileCheck.exists) {
      return {
        success: false,
        message: fileCheck.message,
      };
    }

    const metaData = {
      'Content-Type': fileType,
      'X-Upload-Id': crypto.randomUUID(),
      ...customMetadata,
    };

    await minioClient.putObject(bucket, destinationObject, fileBuffer, fileBuffer.length, metaData);
    const successMessage = `File buffer ${customMetadata} uploaded as object ${destinationObject} in bucket ${bucket}`;
    logger.info(successMessage);

    return {
      success: true,
      message: successMessage,
    };
  } catch (error) {
    const errorMessage = `Error uploading file: ${error instanceof Error ? error.message : String(error)}`;
    logger.error(errorMessage);
    return {
      success: false,
      message: errorMessage,
    };
  }
}

export async function createMinioBucketListeners(listOfBuckets: string[]) {
  for (const bucket of listOfBuckets) {
    if (registeredBuckets.has(bucket)) {
      logger.debug(`Bucket ${bucket} already registered`);
      continue;
    }

    const listener = minioClient.listenBucketNotification(bucket, prefix, suffix, [
      's3:ObjectCreated:*',
    ]);

    registeredBuckets.add(bucket);

    logger.info(`Listening for notifications on bucket ${bucket}`);

    listener.on('notification', async (notification) => {
      //@ts-ignore
      const file = notification.s3.object.key;

      //@ts-ignore
      const tableName = notification.s3.bucket.name;

      logger.info(`File received: ${file} from bucket ${tableName}`);

      try {
        await minioClient.fGetObject(bucket, file, `tmp/${file}`);

        const fileBuffer = await readFile(`tmp/${file}`);

        //get the file extension
        const extension = file.split('.').pop();
        logger.info(`File Downloaded - Type: ${extension}`);

        if (extension === 'json' && validateJsonFile(fileBuffer)) {
          logger.info('File is a valid json file');

          // Construct the S3-style URL for the file
          const minioUrl = `http://${endPoint}:${port}/${bucket}/${file}`;

          const key = getFirstField(JSON.parse(fileBuffer.toString()));

          // Create table from json
          await createTableFromJson(minioUrl, { accessKey, secretKey }, tableName, key);

          // Insert data into clickhouse
          await insertFromS3Json(tableName, minioUrl, {
            accessKey,
            secretKey,
          });
        } else if (extension === 'csv' && getCsvHeaders(fileBuffer)) {
          //get the first line of the csv file
          const fields = (await readFile(`tmp/${file}`, 'utf8')).split('\n')[0].split(',');

          await createTable(fields, tableName);

          // If running locally and using docker compose, the minio host is 'minio'. This is to allow clickhouse to connect to the minio server

          // Construct the S3-style URL for the file
          const minioUrl = `http://${endPoint}:${port}/${bucket}/${file}`;

          // Insert data into clickhouse
          await insertFromS3(tableName, minioUrl, {
            accessKey,
            secretKey,
          });
        } else {
          logger.warn(`Unknown file type - ${extension}`);
        }
        await rm(`tmp/${file}`);
        logger.debug(`File ${file} deleted from tmp directory`);
      } catch (error) {
        logger.error(`Error processing file ${file}: ${error}`);
      }
    });
  }
}

export function sanitizeBucketName(name: string) {
  return name.toLowerCase()
    .replace(/[^a-z0-9-]/g, '')
    .replace(/^-+|-+$/g, '')
    .replace(/\.{2,}/g, '.')
    .substring(0, 63);
}
