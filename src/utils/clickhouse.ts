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
    console.debug(`Creating table ${normalizedTableName} with fields ${fields.join(', ')}`);
    const result = await client.query({
      query: generateDDL(fields, normalizedTableName),
    });
    console.log('Table created successfully');
  } catch (error) {
    console.log('Error checking/creating table');
    console.error(error);
    return false;
  }

  await client.close();
  return true;
}

export function generateDDL(fields: string[], tableName: string) {
  return `CREATE TABLE IF NOT EXISTS ${tableName} (
    table_id UUID DEFAULT generateUUIDv4(),
    ${fields.map((field) => `${field} VARCHAR`).join(', ')}
  )
  ENGINE = MergeTree
  ORDER BY (table_id)
  `;
}

export function flattenJson(json: any, prefix = ''): string[] {
  const fields: string[] = [];
  Object.keys(json).forEach((key) => {
    const value = json[key];
    if (typeof value === 'object') {
      if (key === 'main') {
        fields.push(...flattenJson(value));
      } else {
        // This is to avoid having a prefix starting with numbers
        if (Array.isArray(json)) {
          fields.push(...flattenJson(value, prefix));
        } else {
          fields.push(...flattenJson(value, `${key}_`));
        }
      }
    } else {
      fields.push(`${prefix}${key}`);
    }
  });
  const fieldsSet = new Set(fields);
  return Array.from(fieldsSet);
}
