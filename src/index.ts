import express from 'express';
import { getConfig } from './config/config';
import logger from './logger';
import routes from './routes/index';
import { getMediatorConfig, initializeBuckets, setupMediator } from './openhim/openhim';

const app = express();

app.use('/', routes);

app.listen(getConfig().port, async () => {
  logger.info(`Server is running on port - ${getConfig().port}`);

  if (getConfig().runningMode !== 'testing' && getConfig().registerMediator) {
    await setupMediator();

    const mediatorConfig = await getMediatorConfig();
    if (mediatorConfig) {
      await initializeBuckets(mediatorConfig);
    } else {
      logger.error('Failed to fetch mediator config');
    }
  } else {
    logger.info('Running in testing mode, skipping mediator setup');
  }
});
