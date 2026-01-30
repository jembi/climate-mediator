import { createClient } from '@clickhouse/client';
import express from 'express';
import { getConfig } from './config/config';
import logger from './logger';
import { datalake } from './datalake';
import { getMediatorConfig, setupMediator } from './openhim/openhim';
import routes from './routes/index';
// import { MinioBucketsRegistry } from './types/mediatorConfig'; // Removed as its no longer used directly here
import { setupClickhouseTables } from './utils/clickhouse';
import { setupKafkaConsumers } from './utils/kafka';

const app = express();

app.use('/', routes);

let server;
if (require.main === module) {
  server = app.listen(getConfig().port, async () => {
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
      await setupClickhouseTables();
    }

    client.close();

    await setupKafkaConsumers();

    logger.info(`Server is running on port - ${getConfig().port}`);
    logger.debug(`Running in ${getConfig().runningMode} mode`);

    if (getConfig().runningMode !== 'testing' && getConfig().registerMediator) {
      await setupMediator();

      // Initialize datalake listeners
      await datalake.listeners.startListening(
        Array.from(datalake.buckets.getRegisteredBuckets())
      );
    } else {
      logger.info('Running in testing mode, skipping mediator setup');
    }
  });
}

export { app, server };
export default app;
