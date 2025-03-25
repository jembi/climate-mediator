import { createClient } from '@clickhouse/client';
import express from 'express';
import { getConfig } from './config/config';
import logger from './logger';
import { getMediatorConfig, initializeBuckets, setupMediator } from './openhim/openhim';
import routes from './routes/index';
import { MinioBucketsRegistry } from './types/mediatorConfig';
import { setupClickhouseTables, createHistoricalDiseaseTable, createPopulationTable } from './utils/clickhouse';

const app = express();

app.use('/', routes);

app.listen(getConfig().port, async () => {
  const {
    clickhouse: { url, password },
  } = getConfig();

  const client = createClient({
    url,
    password,
  });

  const result = await client.ping();

  if (!result.success) {
    logger.error(
      'Connection to ClickHouse failed. Verify your ClickHouse credentials and ensure the ClickHouse instance is online'
    );
  } else {
    logger.debug('Connection to ClickHouse successful');
    await createHistoricalDiseaseTable();
    await createPopulationTable();
    await setupClickhouseTables();
  }

  client.close();

  logger.info(`Server is running on port - ${getConfig().port}`);
  logger.debug(`Running in ${getConfig().runningMode} mode`);

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
