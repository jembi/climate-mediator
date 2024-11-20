import express from 'express';
import path from 'path';
import { getConfig } from './config/config';
import logger from './logger';
import routes from './routes/index';
import { setupMediator } from './openhim/openhim';
import { setupMinio } from './utils/minio';

const app = express();

const prefix = getConfig().runningMode === 'testing' ? '/' : '/climate';

app.use(prefix, routes);

setupMinio();

app.listen(getConfig().port, () => {
  logger.info(`Server is running on port - ${getConfig().port}`);

  if (getConfig().runningMode !== 'testing' && getConfig().registerMediator) {
    setupMediator(path.resolve(__dirname, './openhim/mediatorConfig.json'));
  }
});
