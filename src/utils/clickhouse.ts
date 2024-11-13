import { createClient } from '@clickhouse/client';
import { getConfig } from '../config/config';

const { clickhouse } = getConfig();
const { url } = clickhouse;


export async function createTable(fields: string[], tableName: string) {
  const client = createClient({
    url,
  });
  await client.query({
    query: generateDDL(fields, tableName),
  });

  await client.close();
}

export function generateDDL(fields: string[], tableName: string) {
  return `CREATE TABLE IF NOT EXISTS ${tableName} (
    ${fields.map((field) => `${field} VARCHAR`).join(", ")}
  )
  ENGINE = MergeTree`;
}

export function flattenJson(json: any, prefix = ""): string[] {
  const fields: string[] = [];
  Object.keys(json).forEach((key) => {
    const value = json[key];
    if (typeof value === "object") {
      if (key === "main") {
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