import * as Minio from 'minio';
import { getConfig } from '../config/config';
import logger from '../logger';
import { readFile, rm } from 'fs/promises';
import { createTable, insertFromS3 } from './clickhouse';
import { validateJsonFile, getCsvHeaders } from './file-validators';

const { endPoint, port, useSSL, bucketRegion, accessKey, secretKey, prefix, suffix, buckets } =
  getConfig().minio;

/**
 * Uploads a file to Minio storage
 * @param {string} sourceFile - Path to the file to upload
 * @param {string} destinationObject - Name for the uploaded object
 * @param {Object} [customMetadata={}] - Optional custom metadata
 * @returns {Promise<void>}
 */
export async function uploadToMinio(
  sourceFile: string,
  destinationObject: string,
  bucket: string,
  fileType: string,
  customMetadata = {}
) {
  const minioClient = new Minio.Client({
    endPoint,
    port,
    useSSL,
    accessKey,
    secretKey,
  });
  // Check if bucket exists, create if it doesn't
  const exists = await minioClient.bucketExists(bucket);
  if (!exists) {
    await minioClient.makeBucket(bucket, bucketRegion);
    logger.info(`Bucket ${bucket} created in "${bucketRegion}".`);
  }

  try {
    const fileExists = await checkFileExists(destinationObject, bucket, fileType);
    if (fileExists) {
      return false;
    } else {
      const metaData = {
        'Content-Type': fileType,
        'X-Upload-Id': crypto.randomUUID(),
        ...customMetadata,
      };

      // Upload the file
      await minioClient.fPutObject(bucket, destinationObject, sourceFile, metaData);
      logger.info(
        `File ${sourceFile} uploaded as object ${destinationObject} in bucket ${bucket}`
      );
      return true;
    }
  } catch (error) {
    console.error('Error checking file:', error);
  }
}

/**
 * Checks if a CSV file exists in the specified Minio bucket
 * @param {string} fileName - Name of the CSV file to check
 * @param {string} bucket - Bucket name
 * @returns {Promise<boolean>} - Returns true if file exists, false otherwise
 */
export async function checkFileExists(
  fileName: string,
  bucket: string,
  fileType: string
): Promise<boolean> {
  const minioClient = new Minio.Client({
    endPoint,
    port,
    useSSL,
    accessKey,
    secretKey,
  });

  try {
    // Check if bucket exists first
    const bucketExists = await minioClient.bucketExists(bucket);
    if (!bucketExists) {
      logger.info(`Bucket ${bucket} does not exist`);
      return false;
    }

    // Get object stats to check if file exists
    const stats = await minioClient.statObject(bucket, fileName); // Optionally verify it's a CSV file by checking Content-Type
    if (stats.metaData && stats.metaData['content-type'] === fileType) {
      logger.info(`File ${fileName} exists in bucket ${bucket}`);
      return true;
    } else {
      logger.info(`File ${fileName} does not exist in bucket ${bucket}`);
      return false;
    }
  } catch (err: any) {
    if (err.code === 'NotFound') {
      logger.debug(`File ${fileName} not found in bucket ${bucket}`);
      return false;
    }
    // For any other error, log it and rethrow
    logger.error(`Error checking file existence: ${err.message}`);
    throw err;
  }
}

export async function createMinioBucketListeners() {
  const minioClient = new Minio.Client({
    endPoint,
    port,
    useSSL,
    accessKey,
    secretKey,
  });

  try {
    // Test connection by attempting to list buckets
    await minioClient.listBuckets();
    logger.info(`Successfully connected to Minio at ${endPoint}:${port}`);
  } catch (error) {
    logger.error(`Failed to connect to Minio: ${error}`);
    throw error;
  }

  const listOfBuckets = buckets.split(',');

  for (const bucket of listOfBuckets) {
    const listener = minioClient.listenBucketNotification(bucket, prefix, suffix, [
      's3:ObjectCreated:*',
    ]);

    logger.debug(`Listening for notifications on bucket ${bucket}`);

    listener.on('notification', async (notification) => {
      
      //@ts-ignore
      const file = notification.s3.object.key;
      
      //@ts-ignore
      const tableName = notification.s3.bucket.name;

      logger.info(`File received: ${file} from bucket ${tableName}`);

      //@ts-ignore
      minioClient.fGetObject(bucket, file, `tmp/${file}`, async (err) => {
        if (err) {
          logger.error(err);
        } else {
          const fileBuffer = await readFile(`tmp/${file}`);

          //get the file extension
          const extension = file.split('.').pop();
          logger.info(`File Downloaded - Type: ${extension}`);

          if (extension === 'json' && validateJsonFile(fileBuffer)) {
            // flatten the json and pass it to clickhouse
            //const fields = flattenJson(JSON.parse(fileBuffer.toString()));
            //await createTable(fields, tableName);
            logger.warn(`File type not currently supported- ${extension}`);
          } else if (extension === 'csv' && getCsvHeaders(fileBuffer)) {
            //get the first line of the csv file
            const fields = (await readFile(`tmp/${file}`, 'utf8')).split('\n')[0].split(',');

            await createTable(fields, tableName);

            // If running locally and using docker compose, the minio host is 'minio'. This is to allow clickhouse to connect to the minio server
            const host = getConfig().runningMode === 'testing' ? 'minio' : endPoint;
            // Construct the S3-style URL for the file
            const minioUrl = `http://${host}:${port}/${bucket}/${file}`;

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
        }
      });
    });
  }
}
