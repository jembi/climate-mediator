import { EachMessagePayload } from 'kafkajs';
import logger from '../../logger';
import { createGenericTable, genericInsertIntoTable } from '../../utils/clickhouse';

export async function processMessageByTopic(
  messagePayload: EachMessagePayload
): Promise<void> {
  const { topic } = messagePayload;

  switch (topic) {
    case 'disease-case-topic':
      await handleDiseaseCaseMessage(messagePayload);
      break;
    default:
      logger.warn(`No handler found for topic ${topic}`);
  }
}

async function handleDiseaseCaseMessage(messagePayload: EachMessagePayload): Promise<void> {
  logger.debug('Processing disease case message');

  try {
    const messagePayloadJson = messagePayload.message.value?.toString('utf8') || '';
    const bodyFromOpenhim = JSON.parse(JSON.parse(messagePayloadJson)?.body);

    if (!bodyFromOpenhim) {
      logger.error('No body found in the message payload');
      return;
    }

    const tableName = messagePayload.topic.replace(/[^a-zA-Z0-9_]/g, '_');
    const { schema, orderBy } = generateSchema(bodyFromOpenhim);
    const jsonData = bodyFromOpenhim;

    (await createGenericTable(tableName, schema, orderBy)) &&
      (await genericInsertIntoTable(tableName, jsonData));
  } catch (err) {
    logger.error(`Error processing disease case message: ${err}`);
    throw err;
  }
}

function generateSchema(parsedJson: any) {
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
    else type = 'String';

    schemas.push(`${key} ${type}`);

    if (!firstColumn) firstColumn = key;
  });

  const schema = schemas.join(',');

  return { schema, orderBy: firstColumn };
}
