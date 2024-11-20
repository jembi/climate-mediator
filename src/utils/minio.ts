import * as Minio from 'minio';
import { getConfig } from '../config/config';
import logger from '../logger';
import { readFile, rm } from 'fs/promises';
import { createTable } from './clickhouse';
import { validateJsonFile, getCsvHeaders } from './file-validators';

export async function setupMinio() {
  const { buckets, endPoint, port, useSSL, accessKey, secretKey, prefix, suffix } =
    getConfig().minio;

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

    listener.on('notification', async (notification) => {
      //@ts-ignore
      const file = notification.s3.object.key;

      //@ts-ignore
      const tableName = notification.s3.bucket.name;

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
          } else {
            logger.warn(`Unknown file type - ${extension}`);
          }
          await rm(`tmp/${file}`);
        }
      });
    });
  }
}
