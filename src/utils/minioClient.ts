import axios from 'axios';
import { createWriteStream } from 'fs';
import fs from 'fs/promises';
import path from 'path';
import { getConfig } from '../config/config';
import { datalake } from '../datalake';
import logger from '../logger';
import { getOpenhimConfig, triggerProcessing } from '../openhim/openhim';
import {
  createTable,
  createTableFromJson,
  insertFromS3,
  insertFromS3Json,
} from './clickhouse';
import { getCsvHeaders, validateJsonFile } from './file-validators';

// Re-export common types and errors from datalake library
import { BucketDoesNotExistError, BucketManager } from '@jembi/openhim-datalake-lib';
export { BucketDoesNotExistError, BucketManager };

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
 * @deprecated Use datalake.buckets.ensureExists instead
 */
export async function ensureBucketExists(
  bucket: string,
  createBucketIfNotExists = false
): Promise<void> {
  await datalake.buckets.ensureExists(bucket, createBucketIfNotExists);
}

/**
 * Uploads a file to Minio storage
 * @deprecated Use datalake.upload.uploadFromPath instead
 */
export async function uploadToMinio(
  sourceFile: string,
  destinationObject: string,
  bucket: string,
  fileType: string,
  customMetadata = {}
) {
  return datalake.upload.uploadFromPath(sourceFile, destinationObject, fileType, {
    bucket,
    customMetadata,
  });
}

/**
 * Uploads a file buffer to Minio storage
 * @deprecated Use datalake.upload.uploadBuffer instead
 */
export async function uploadFileBufferToMinio(
  fileBuffer: Buffer,
  destinationObject: string,
  bucket: string,
  fileType: string,
  customMetadata = {}
) {
  return datalake.upload.uploadBuffer(fileBuffer, destinationObject, fileType, {
    bucket,
    customMetadata,
  });
}

export async function minioListenerHandler(bucket: string, file: string, tableName: string) {
  // Use datalake client to get object
  await datalake.client.fGetObject(bucket, file, `tmp/${file}`);

  const fileBuffer = await readFile(`tmp/${file}`);
  const { endPoint, port, accessKey, secretKey } = getConfig().minio;

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

    // Construct the S3-style URL for the file
    const minioUrl = `http://${endPoint}:${port}/${bucket}/${file}`;

    await createTable(minioUrl, { accessKey, secretKey }, tableName);

    // Insert data into clickhouse
    await insertFromS3(tableName, minioUrl, {
      accessKey,
      secretKey,
    });
  } else {
    logger.warn(`Unknown file type - ${extension}`);
  }
  await fs.unlink(`tmp/${file}`);
  logger.debug(`File ${file} deleted from tmp directory`);
}

/**
 * Downloads the climate data (json or csv type) and uploads it into the minio buckets
 * @param {string} bucket the name of the bucket to donwload data for. If not specified all the buckets will be processed
 */
export async function downloadFileAndUpload(bucket: string | undefined) {
  const buckets = getOpenhimConfig();

  // Check if buckets array exists and has items
  if (!buckets || !Array.isArray(buckets) || buckets.length === 0) {
    logger.warn('No buckets configured in OpenHIM config');
    return { error: 'No buckets configured in OpenHIM config' };
  }

  logger.info(`Buckets: ${buckets}`);
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
      headers,
    });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');

    const fileUrl = path.join(tmpDir, `${timestamp}-${bucketDetails.fileName}`);
    const writer = createWriteStream(fileUrl);
    response.data.pipe(writer);

    await new Promise((resolve, reject) => {
      writer.on('finish', () => resolve(1));
      writer.on('error', (error) => reject(error));
    });

    await fs.appendFile(fileUrl, '\n');

    logger.info('Download finished');

    const uploadResult = await datalake.upload.uploadFromPath(
      fileUrl,
      `${timestamp}-${bucketDetails.fileName}`,
      bucketDetails.fileName.split('.').pop() === 'csv' ? 'text/csv' : 'text/json',
      { bucket: bucketDetails.bucket }
    );

    if (!uploadResult.success) {
      throw Error(`Upload to bucket ${bucketDetails.bucket} failed - ${uploadResult.message}`);
    }
    // uploadFromPath doesn't delete the file, so we do it here
    await fs.unlink(fileUrl);
  }
}

export function sanitizeBucketName(name: string) {
  return BucketManager.sanitizeName(name);
}

// Helpers for readFile
async function readFile(filePath: string, encoding?: BufferEncoding): Promise<any> {
  if (encoding) {
    return fs.readFile(filePath, { encoding });
  }
  return fs.readFile(filePath);
}
