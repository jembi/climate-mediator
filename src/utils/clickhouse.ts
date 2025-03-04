import { createClient } from '@clickhouse/client';
import { getConfig } from '../config/config';
import logger from '../logger';
import { log } from 'console';

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

export async function createTableFromJson(
  s3Path: string,
  s3Config: { accessKey: string; secretKey: string },
  tableName: string,
  key: string
) {
  const client = createClient({
    url,
    password,
  });

  const ping = await client.ping();

  logger.info(`Clickhouse Ping: ${ping.success}`);

  const normalizedTableName = tableName.replace(/-/g, '_');

  //check if the table exists
  try {
    const existsResult = await client.query({
      query: `desc ${normalizedTableName}`,
    });
    logger.info(`Table ${normalizedTableName} already exists`);
    await client.close();
    return false;
  } catch (error) {
    logger.info(`Table ${normalizedTableName} does not exist`);
  }

  try {

    logger.info(`Creating table from JSON ${normalizedTableName}`);

    const query = generateDDLFromJson(s3Path, s3Config, normalizedTableName, key);
    const res = await client.query({ query });

    logger.info(`Successfully created table from JSON ${normalizedTableName}`);
    logger.info(res);

    await client.close();

    return true;
  } catch (err) {
    logger.error(`Error creating table from JSON ${normalizedTableName}`);
    logger.error(err);
    return false;
  }
  
}

export function generateDDL(fields: string[], tableName: string) {
  return `CREATE TABLE ${tableName} (table_id UUID DEFAULT generateUUIDv4(),${fields.map((field) => `${field} VARCHAR`).join(', ')}) ENGINE = MergeTree ORDER BY (table_id)`;
}

export function generateDDLFromJson(
  s3Path: string,
  s3Config: { accessKey: string; secretKey: string },
  tableName: string,
  key: string
) {
  const query = `
  CREATE TABLE IF NOT EXISTS \`default\`.${tableName}
  ENGINE = MergeTree
  ORDER BY ${key} EMPTY
  AS SELECT * 
  FROM s3('${s3Path}', '${s3Config.accessKey}', '${s3Config.secretKey}', JSONEachRow)
  SETTINGS schema_inference_make_columns_nullable = 0
  `;

  logger.info(`Query: ${query}`);

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

function isNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function checkType(feature: any):
  {type: 'point', latitude: number, longitude: number} |
  {type: 'polygon', coordinates: [[number, number]]}
{
  const type = feature?.geometry?.type?.toLowerCase();

  if (type == 'point') {
    if (Array.isArray(feature?.geometry?.coordinates) &&
        feature.geometry.coordinates.every(isNumber)) {
      return {
        type: 'point',
        latitude: feature.geometry.coordinates[0],
        longitude: feature.geometry.coordinates[1],
      };
    }
  }

  if (type == 'polygon') {
    if (Array.isArray(feature?.geometry?.coordinates?.[0]) &&
        feature.geometry.coordinates?.[0].every(Array.isArray)) {
      return {
        type : 'polygon',
        coordinates: feature.geometry.coordinates[0] as any,
      };
    }
  }

  throw new Error('Invalid geometry type', {cause: feature});
}

export async function insertOrganizationIntoTable(
  tableName: string,
  payload: string,
) {
  const client = createClient({
    url,
    password,
  });

  const normalizedTableName = tableName.replace(/-/g, '_');

  logger.info(`Inserting data into ${normalizedTableName}`);

  try {
    const json = JSON.parse(payload);

    const values = json.orgUnitsGeoJson.features.map((feature: any) => {
      const type = checkType(feature);

      return {
        code: feature.properties.code,
        name: feature.properties.name,
        level: feature.properties.level,
        type: type.type,
        latitude: type.type == 'point' ? type.latitude : null,
        longitude: type.type == 'point' ? type.longitude : null,
        coordinates: type.type == 'polygon' ? type.coordinates : null,
      };
    });

    await client.insert({
      table: 'default.' + normalizedTableName,
      values,
      format: 'JSONEachRow',
    })

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

export async function createOrganizationsTable(
  tableName: string,
) {
  const normalizedTableName = tableName.replace(/-/g, '_');

  logger.info(`Creating Organizations table from JSON ${normalizedTableName}`);

  const client = createClient({
    url,
    password,
  });

  //check if the table exists
  try {
    const existsResult = await client.query({
      query: `desc ${normalizedTableName}`,
    });
    logger.info(`Table ${normalizedTableName} already exists`);
    await client.close();
    return false;
  } catch (error) {
  }

  try {
    
    const query = `
      CREATE TABLE IF NOT EXISTS \`default\`.${normalizedTableName}
      ( code String,
       name String,
       level String,
       type String,
       latitude Float32,
       longitude Float32,
       coordinates Array(Array(Float32))
      )
      ENGINE = MergeTree
      ORDER BY code
    `;

    logger.info(query);

    const res = await client.query({ query });

    logger.info(`Successfully created table from JSON ${normalizedTableName}`);
    logger.info(res);

    await client.close();

    return true;
  } catch (err) {
    logger.error(`Error creating table from JSON ${normalizedTableName}`);
    logger.error(err);
    return false;
  }
  
}