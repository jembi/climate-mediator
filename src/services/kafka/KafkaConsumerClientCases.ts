import winston from 'winston';
import { createGenericTable, generateInsertIntoTable } from '../../utils/clickhouse';
import { KafkaConsumer } from './KafkaConsumer';

export class KafkaConsumerClientCases implements KafkaConsumer {
  constructor(private readonly logger: winston.Logger) {}

  async getTopicID(): Promise<string> {
    return 'disease-case-topic';
  }

  async onConsumeMessage(messagePayloadJson: string): Promise<void> {
    this.logger.debug(`Consuming message on KafkaConsumerClientCases`);

    try {
      const bodyFromOpenhim = JSON.parse(JSON.parse(messagePayloadJson)?.body);

      if (!bodyFromOpenhim) {
        this.logger.error('No body found in the message payload');
        return;
      }

      // @todo - get this dynamically?
      const tableName = 'disease_case_kafka';
      const { schema, orderBy } = this.generateSchema(bodyFromOpenhim);
      const jsonData = bodyFromOpenhim;

      await createGenericTable(tableName, schema, orderBy);
      await generateInsertIntoTable(tableName, jsonData);
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

      if (typeof value === 'number') type = 'Int64';
      else if (typeof value === 'boolean') type = 'Boolean';
      else type = 'String';

      schemas.push(`${key} ${type}`);

      if (!firstColumn) firstColumn = key;
    });

    const schema = schemas.join(',');

    return { schema, orderBy: firstColumn };
  }
}
