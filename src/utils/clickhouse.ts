import { createClient } from '@clickhouse/client';
import { getConfig } from '../config/config';
import logger from '../logger';

const { clickhouse } = getConfig();
const { url, password } = clickhouse;

export async function createTable(fields: string[], tableName: string) {
  const client = createClient({
    url,
    password,
  });

  const normalizedTableName = tableName.replace(/-/g, '_');

  try {
    logger.debug(`Checking if table ${normalizedTableName} exists...`);
    const existsResult = await client.query({
      query: `desc ${normalizedTableName}`,
    });
    logger.debug(`Table ${normalizedTableName} exists`);
    await client.close();
    return false;
  } catch (error) {
    logger.debug(`Table ${normalizedTableName} does not exist, now creating...`);
  }

  try {
    logger.debug(`Creating table ${normalizedTableName} with fields ${fields.join(', ')}`);
    const result = await client.query({
      query: generateDDL(fields, normalizedTableName),
    });
    logger.info(`Table ${normalizedTableName} created successfully`);
  } catch (error) {
    logger.error(`Error checking/creating table ${normalizedTableName}`);
    logger.debug(JSON.stringify(error));
    return false;
  }

  await client.close();
  return true;
}

/**
 * Create a table within clickhouse from the inferred schema from the json file
 * if table already exists within the clickhouse function will return false
 * 
 * @param s3Path URL location of the json within Minio
 * @param s3Config Access key and Secrete key credentials to access Minio
 * @param tableName The name of the table to be created within Minio
 * @param groupByColumnName The column the created table will be ORDERED By within clickhouse
 * @returns 
 */

export async function createTableFromJson(
  s3Path: string,
  s3Config: { accessKey: string; secretKey: string },
  tableName: string,
  groupByColumnName: string
) {
  const client = createClient({
    url,
    password,
  });

  const normalizedTableName = tableName.replace(/-/g, '_');

  try {
    const existsResult = await client.query({
      query: `desc ${normalizedTableName}`,
    });
    logger.debug(`Table ${normalizedTableName} already exists`);
    await client.close();
    return false;
  } catch (error) {
    logger.debug(`Table ${normalizedTableName} does not exist`);
  }

  const query = generateDDLFromJson(s3Path, s3Config, normalizedTableName, groupByColumnName);
  await client.query({ query });
  await client.close();
}

export function generateDDL(fields: string[], tableName: string) {
  return `CREATE TABLE ${tableName} (table_id UUID DEFAULT generateUUIDv4(),${fields.map((field) => `${field} VARCHAR`).join(', ')}) ENGINE = MergeTree ORDER BY (table_id)`;
}

export function generateDDLFromJson(
  s3Path: string,
  s3Config: { accessKey: string; secretKey: string },
  tableName: string,
  groupByColumnName: string
) {
  const query = `
  CREATE TABLE IF NOT EXISTS \`default\`.${tableName}
  ENGINE = MergeTree
  ORDER BY ${groupByColumnName} EMPTY
  AS SELECT * 
  FROM s3('${s3Path}', '${s3Config.accessKey}', '${s3Config.secretKey}', JSONEachRow)
  SETTINGS schema_inference_make_columns_nullable = 0
  `;
  return query;
}

export async function insertFromS3(
  tableName: string,
  s3Path: string,
  s3Config: {
    accessKey: string;
    secretKey: string;
  }
) {
  logger.info(`Inside the insertFromS3 function`);
  const client = createClient({
    url,
    password,
  });
  logger.info(`s3Path: ${s3Path}`);
  const normalizedTableName = tableName.replace(/-/g, '_');

  try {
    logger.debug(`Inserting data into ${normalizedTableName} from ${s3Path}`);
    const query = `
      INSERT INTO \`default\`.${normalizedTableName} 
      SELECT * FROM s3(
        '${s3Path}',
        '${s3Config.accessKey}',
        '${s3Config.secretKey}',
        'CSVWithNames'
      )
    `;
    logger.debug(`Query: ${query}`);
    await client.query({ query });
    logger.info(`Successfully inserted data into ${normalizedTableName}`);
    return true;
  } catch (error) {
    logger.error('Error inserting data from S3');
    logger.error(error);
    return false;
  } finally {
    await client.close();
  }
}

export async function insertFromS3Json(
  tableName: string,
  s3Path: string,
  s3Config: {
    accessKey: string;
    secretKey: string;
  }
) {
  const client = createClient({
    url,
    password,
  });

  const normalizedTableName = tableName.replace(/-/g, '_');

  try {
    logger.debug(`Inserting data into ${normalizedTableName}`);
    const query = `
      INSERT INTO \`default\`.${normalizedTableName} 
      SELECT * FROM s3(
        '${s3Path}',
        '${s3Config.accessKey}',
        '${s3Config.secretKey}',
        'JSONEachRow'
      )
    `;
    await client.query({ query });
    logger.info(`Successfully inserted data into ${normalizedTableName}`);
    return true;
  } catch (error) {
    logger.error('Error inserting data from JSON');
    logger.error(error);
    return false;
  } finally {
    await client.close();
  }
}
