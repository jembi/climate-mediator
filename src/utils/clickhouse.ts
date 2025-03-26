import { createClient } from '@clickhouse/client';
import { getConfig } from '../config/config';
import logger from '../logger';
import { HistoricData, PopulationData } from './file-validators';
import { cli } from 'winston/lib/winston/config';

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
  
  createGenericTable(tableName, '', groupByColumnName);
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
    logger.info(`Table ${normalizedTableName} does not exist`);
  }

  try {
    logger.info(`Creating table from JSON ${normalizedTableName}`);

    const query = generateDDLFromJson(
      s3Path,
      s3Config,
      normalizedTableName,
      groupByColumnName
    );
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
        ${s3Path.split('.').pop() === 'json' ? 'JSONEachRow' : 'CSVWithNames'}
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

export async function createHistoricalDiseaseTable() {
  const schema = `organizational_unit String,
     period String,
     value Int64`;
  return createGenericTable(
    'historical_disease',
    schema,
    'organizational_unit'
  );
}

export async function createOrganizationsTable() {
  const schema = `code String,
     name String,
     level String,
     type String,
     latitude Float32,
     longitude Float32,
     coordinates Array(Array(Float32)),
     timestamp UInt64`;
  return createGenericTable(
    'organizations',
    schema,
    'name'
  );
}

export async function createPopulationTable() {
  const schema = `organizational_unit String,
     period String,
     value Int64`;  
  return createGenericTable(
    'population_data',
    schema,
    'organizational_unit'
  );
}

export async function insertHistoricDiseaseData(
  diseaseData: HistoricData[]
) {
  const client = createClient({
    url,
    password,
  });

  try {
    logger.debug('Now inserting data');
    await client.insert({
      table: 'historical_disease',
      values: diseaseData,
      format: 'JSONEachRow',
    });
    logger.debug('Insertion successful');
  } catch (error) {
    logger.error('There was an issue inserting the data into clickhouse: ' + JSON.stringify(error));
  }
  return client.close();
}

export async function insertPopulationData(
  populationData: PopulationData[]
) {
  const client = createClient({
    url,
    password,
  });
  try {
    logger.debug('Now inserting data');
    await client.insert({
      table: 'population_data',
      values: populationData,
      format: 'JSONEachRow',
    });
    logger.debug('Insertion successful');
  } catch (error) {
    logger.error('There was an issue inserting the data into clickhouse: ' + JSON.stringify(error));
  }
  return client.close();
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
        latitude: feature.geometry.coordinates[1],
        longitude: feature.geometry.coordinates[0],
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

  throw new Error('Invalid geometry type. ' + JSON.stringify(feature));
}

export async function setupClickhouseTables() {
  try {
    await createHistoricalDiseaseTable();
    await createOrganizationsTable();
    await createPopulationTable();
  } catch (err) {
    logger.error('Error setting up Clickhouse tables');
    logger.error(err);
  }
}

export async function insertOrganizationIntoTable(
  payload: string,
) {
  const client = createClient({
    url,
    password,
  });

  const normalizedTableName = 'organizations';

  logger.info(`Inserting data into ${normalizedTableName}`);

  try {
    const json = JSON.parse(payload);
    const timestamp = Date.now();

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
        timestamp,
      };
    });

    await client.insert({
      table: 'default.' + normalizedTableName,
      values,
      format: 'JSONEachRow',
      clickhouse_settings: {
        optimize_on_insert: 1,
      }
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

export interface ClickhouseOrganzation {
  code: string;
  name: string;
  level: number;
  type: 'point' | 'polygon';
  latitude: number;
  longitude: number;
  coordinates: [[number, number]];
  timestamp: number;
}

export interface ClickhouseHistoricalDisease {
  organizational_unit: string;
  period: string;
  value: number;
}


export async function fetchOrganizations() {
  const client = createClient({
    url,
    password,
  });

  try {
    const query = `
      SELECT * FROM default.organizations
    `;

    const res = await (await client.query({ query})).json();

    return res.data as ClickhouseOrganzation[];

  } catch (err) {
    logger.error('Error fetching organizations');
    logger.error(err);
    throw err;
  }
}

export async function fetchHistoricalDisease() {
  const client = createClient({
    url,
    password,
  });

  try {
    const query = `
      SELECT DISTINCT ON(historical_disease.organizational_unit, historical_disease.period) hd.*
      FROM historical_disease hd
      INNER JOIN organizations oo ON oo.name = hd.organizational_unit
    `;

    const res = await (await client.query({ query})).json();

    return res.data as ClickhouseHistoricalDisease[];

  } catch (err) {
    logger.error('Error fetching historical_disease');
    logger.error(err);
    throw err;
  }
}

/**
 * Generic function to create a table in ClickHouse
 * 
 * @param tableName Name of the table to create
 * @param schema SQL schema definition for the table columns
 * @param orderBy Column(s) to order the table by
 * @param engine Table engine (defaults to MergeTree)
 * @returns Promise<boolean> True if table was created, false otherwise
 */
export async function createGenericTable(
  tableName: string,
  schema: string,
  orderBy: string,
  engine: string = 'MergeTree'
): Promise<boolean> {
  const client = createClient({
    url,
    password,
  });

  try {
    const existsResult = await client.query({
      query: `desc ${tableName}`,
    });
    logger.info(`Table ${tableName} already exists`);
    await client.close();
    return false;
  } catch (error) {
    if (error instanceof Error && 
        error.message.includes('Table') && 
        (error.message.includes('not found') || error.message.includes('does not exist'))) {
      logger.debug(`Table ${tableName} does not exist`);
    } else {
      logger.error(`Error checking if ${tableName} table exists:`);
      logger.error(error);
      await client.close();
      return false;
    }
  }

  try {
    logger.debug(`Now creating table ${tableName}`);
    await client.query({
      query: `
        CREATE TABLE IF NOT EXISTS ${tableName} (
          ${schema}
        ) ENGINE = ${engine}()
        ORDER BY (${orderBy})
      `,
    });
    logger.debug(`${tableName} table created successfully`);
    return true;
  } catch (error) {
    logger.error(`There was an issue creating the table ${tableName} in clickhouse: ${JSON.stringify(error)}`);
    return false;
  } finally {
    await client.close();
  }
}
