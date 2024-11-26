import express from 'express';
import path from 'path';
import { getConfig } from './config/config';
import logger from './logger';
import routes from './routes/index';
import { getRegisteredBuckets, setupMediator } from './openhim/openhim';
import { createMinioBucketListeners, ensureBucketExists } from './utils/minioClient';

const app = express();

app.use('/', routes);

app.listen(getConfig().port, async () => {
  logger.info(`Server is running on port - ${getConfig().port}`);

  if (getConfig().runningMode !== 'testing' && getConfig().registerMediator) {
    await setupMediator();
  }

  const buckets = await getRegisteredBuckets();

  buckets.length === 0 && logger.warn('No buckets specified in the configuration');

  for await (const { bucket, region } of buckets) {
    await ensureBucketExists(bucket, region);
  }

  createMinioBucketListeners(buckets.map((bucket) => bucket.bucket));
});
