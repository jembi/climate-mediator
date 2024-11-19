import express from 'express';
import * as Minio from 'minio';
import path from 'path';
import { getConfig } from './config/config';
import logger from './logger';
import routes from './routes/index';
import { setupMediator } from './openhim/openhim';
import { validateJsonFile, getCsvHeaders } from './utils/file-validators';
import { readFile, rm } from 'fs/promises';
import { createTable, flattenJson, insertFromS3 } from './utils/clickhouse';

const app = express();

app.use('/', routes);

if (getConfig().runningMode !== 'testing') {
  app.listen(getConfig().port, () => {
    logger.info(`Server is running on port - ${getConfig().port}`);

    if (getConfig().registerMediator) {
      setupMediator(path.resolve(__dirname, './openhim/mediatorConfig.json'));
    }
  });
}

async function setupMinio() {
  const { bucket, endPoint, port, useSSL, accessKey, secretKey, prefix, suffix } =
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
    logger.info(`Successfully connected to Minio at ${endPoint}:${port}/${bucket}`);
  } catch (error) {
    logger.error(`Failed to connect to Minio: ${error}`);
    throw error;
  }
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

          // Construct the S3-style URL for the file
          const minioUrl = `http://${endPoint}:${port}/${bucket}/${file}`;

          // First create table
          await createTable(fields, tableName);
          logger.info(`Inserting data into ${tableName} from ${minioUrl}`);

          // Insert data into clickhouse
          await insertFromS3(tableName, minioUrl, {
            accessKey,
            secretKey
          });
          
        } else {
          logger.warn(`Unknown file type - ${extension}`);
        }
        await rm(`tmp/${file}`);
      }
    });
  });
}

setupMinio();
