import express from 'express';
import * as Minio from 'minio';
import path from 'path';
import { getConfig } from './config/config';
import logger from './logger';
import routes from './routes/index';
import { setupMediator } from './openhim/openhim';
import { validateJsonFile, validateCsvFile } from './utils/file-validators';
import { readFile, rm } from 'fs/promises';
import { createTable, flattenJson } from './utils/clickhouse';

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

const { bucket, endPoint, port, useSSL, accessKey, secretKey, prefix, suffix } =
  getConfig().minio;

const minioClient = new Minio.Client({
  endPoint,
  port,
  useSSL,
  accessKey,
  secretKey,
});

const listener = minioClient.listenBucketNotification(bucket, prefix, suffix, [
  's3:ObjectCreated:*',
]);

listener.on('notification', async (notification) => {
  //@ts-ignore
  const file = notification.s3.object.key;
  //TODO: Get the Buckets name from the notification object
  //@ts-ignore
  const tableName = notification.s3.object.key;

  //@ts-ignore
  minioClient.fGetObject(bucket, file, `tmp/${file}`, async (err) => {
    if (err) {
      console.error(err);
    } else {
      console.log('File downloaded');
      const fileBuffer = await readFile(`tmp/${file}`);

      //get the file extension
      const extension = file.split('.').pop();
      console.log(extension);

      if (extension === 'json' && validateJsonFile(fileBuffer)) {
        // flatten the json and pass it to clickhouse
        const fields = flattenJson(JSON.parse(fileBuffer.toString()));
        await createTable(fields, tableName);
      } else if (extension === 'csv' && validateCsvFile(fileBuffer)) {
        // get the first line of the csv file and use it as the fields for the clickhouse table
        const fields = (await readFile(`tmp/${file}`, 'utf8')).split(',');
        await createTable(fields, tableName);
      } else {
        console.log('Unknown file type');
      }
      await rm(`tmp/${file}`);
    }
  });
});
