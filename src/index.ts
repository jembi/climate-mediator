import express from 'express';
import { getConfig } from './config/config';
import logger from './logger';
import routes from './routes/index';
import { getMediatorConfig, initializeBuckets, setupMediator } from './openhim/openhim';
import { MinioBucketsRegistry } from './types/mediatorConfig';

const app = express();

app.use('/', routes);

app.listen(getConfig().port, async () => {
  logger.info(`Server is running on port - ${getConfig().port}`);

  if (getConfig().runningMode !== 'testing' && getConfig().registerMediator) {
    await setupMediator();

    const mediatorConfig = await getMediatorConfig();
    if (mediatorConfig) {
      await initializeBuckets(
        mediatorConfig.config?.minio_buckets_registry as MinioBucketsRegistry[]
      );
    } else {
      logger.warn('Failed to fetch mediator config, skipping bucket initialization');
    }
  } else {
    logger.info('Running in testing mode, skipping mediator setup');
  }
});
