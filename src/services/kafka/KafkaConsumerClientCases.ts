import winston from 'winston';
import { createGenericTable, genericInsertIntoTable } from '../../utils/clickhouse';
import { KafkaConsumer } from './KafkaConsumer';
import { EachMessagePayload } from 'kafkajs';

export class KafkaConsumerClientCases implements KafkaConsumer {
  constructor(private readonly logger: winston.Logger) {}

  async getTopicsInterestedIn(): Promise<string[]> {
    return ['disease-case-topic'];
  }

  async onConsumeMessage(messagePayload: EachMessagePayload): Promise<void> {
    this.logger.debug(`Consuming message on KafkaConsumerClientCases`);

    try {
      const messagePayloadJson = messagePayload.message.value?.toString('utf8') || '';
      const bodyFromOpenhim = JSON.parse(JSON.parse(messagePayloadJson)?.body);

      if (!bodyFromOpenhim) {
        this.logger.error('No body found in the message payload');
        return;
      }

      const tableName = messagePayload.topic.replace(/[^a-zA-Z0-9_]/g, '_');
      const { schema, orderBy } = this.generateSchema(bodyFromOpenhim);
      const jsonData = bodyFromOpenhim;

      (await createGenericTable(tableName, schema, orderBy)) &&
        (await genericInsertIntoTable(tableName, jsonData));
    } catch (err) {
      this.logger.error(`Error processing message in KafkaConsumerClientCases: ${err}`);
      throw err;
    }
  }

  private generateSchema(parsedJson: any) {
    let entry;
    const schemas: string[] = [];
    let firstColumn = '';

    if (Array.isArray(parsedJson)) {
      entry = parsedJson[0];
    } else {
      entry = parsedJson;
    }

    Object.keys(entry).forEach((key) => {
      const value = entry[key];
      let type;

      if (typeof value === 'number' && !Number.isInteger(value)) type = 'Float64';
      else if (typeof value === 'number') type = 'Int64';
      else if (typeof value === 'boolean') type = 'Boolean';
      else if (typeof value === 'string') type = 'String';
      else type = `String`;

      schemas.push(`${key} ${type}`);

      if (!firstColumn) firstColumn = key;
    });

    const schema = schemas.join(',');

    return { schema, orderBy: firstColumn };
  }
}
