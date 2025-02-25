import * as Minio from 'minio';
import axios, { AxiosError } from 'axios';

import { getConfig } from '../config/config';
import logger from '../logger';
import crypto from 'crypto';
import { readFile, rm } from 'fs/promises';
import { createTable, flattenJson, insertFromS3 } from './clickhouse';
import { validateJsonFile, getCsvHeaders } from './file-validators';
import { getOpenhimConfig, triggerProcessing } from '../openhim/openhim';
import fs from 'fs/promises';
import { createWriteStream } from 'fs';
import path from 'path';
import { timeStamp } from 'console';
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
  const exists = await minioClient.bucketExists(bucket);
  if (!exists && createBucketIfNotExists) {
    await minioClient.makeBucket(bucket, region);
    logger.info(
      `Bucket ${bucket} created${region ? `in "${region}"` : ' no region specified'}`
    );
    await createMinioBucketListeners([bucket]);
  }

  if (!exists && !createBucketIfNotExists) {
    throw new BucketDoesNotExistError(`Bucket ${bucket} does not exist`);
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
        await triggerProcessing(bucket, file, tableName);
      } catch (error) {
        logger.error(`Error processing file ${file}: ${error}`);
      }
    });
  }
}

export async function minioListenerHandler (bucket: string, file: string, tableName: string) {
  await minioClient.fGetObject(bucket, file, `tmp/${file}`);

  const fileBuffer = await readFile(`tmp/${file}`);

  //get the file extension
  const extension = file.split('.').pop();
  logger.info(`File Downloaded - Type: ${extension}`);

  if (['json', 'csv'].includes(extension as string)) {
    let fields: string[] = [];

    if (extension === 'json' && validateJsonFile(fileBuffer)) {
      // flatten the json and pass it to clickhouse
      fields = flattenJson(JSON.parse(fileBuffer.toString()));
    } else if (getCsvHeaders(fileBuffer)) {
      fields = (await readFile(`tmp/${file}`, 'utf8')).split('\n')[0].split(',');
    }

    if (fields.length) {
      await createTable(fields, tableName);

      // Construct the S3-style URL for the file
      const minioUrl = `http://${endPoint}:${port}/${bucket}/${file}`;

      // Insert data into clickhouse
      await insertFromS3(tableName, minioUrl, {
        accessKey,
        secretKey,
      });
    }
  } else {
    logger.warn(`Unknown file type - ${extension}`);
  }

  await rm(`tmp/${file}`);
  logger.debug(`File ${file} deleted from tmp directory`);
}

/**
 * Downloads the climate data (json or csv type) and uploads it into the minio buckets
 * @param {string} bucket the name of the bucket to donwload data for. If not specified all the buckets will be processed
 */
export async function downloadFileAndUpload(bucket: string | undefined) {
  const buckets = getOpenhimConfig();

  const tmpDir = path.join(process.cwd(), 'tmp');
  await fs.mkdir(tmpDir, { recursive: true });

  for (let index = 0; index < buckets.length; index++) {
    const bucketDetails = buckets[index];

    logger.info(`Downloading file for bucket - ${bucketDetails.bucket}`);

    if (bucket && bucketDetails.bucket != bucket) continue;

    const headers = bucketDetails.authToken ? { Authorization: bucketDetails.authToken } : {};
    const response = await axios({
      method: 'GET',
      url: bucketDetails.url,
      responseType: 'stream',
      headers
    });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');;

    const fileUrl = path.join(tmpDir, `${timestamp}-${bucketDetails.fileName}`);
    const writer = createWriteStream(fileUrl);
    response.data.pipe(writer);

    await new Promise((resolve, reject) => {
      writer.on('finish', () => resolve(1));
      writer.on('error', error => reject(error));
    });

    await fs.appendFile(fileUrl, '\n');

    logger.info('Download finished');

    const uploadResult = await uploadToMinio(
      fileUrl,
      `${timestamp}-${bucketDetails.fileName}`,
      bucketDetails.bucket,
      bucketDetails.fileName.split('.').pop() === 'csv' ? 'text/csv' : 'text/json'
    );

    if (!uploadResult.success) {
      throw Error(`Upload to bucket ${bucketDetails.bucket} failed - ${uploadResult.message}`);
    }
    await fs.unlink(fileUrl);
  }
}
