import { FileProcessor, ProcessorContext } from '@jembi/openhim-datalake-lib';
import { readFile } from 'fs/promises';
import { getConfig } from '../config/config';
import logger from '../logger';
import {
  createTable,
  createTableFromJson,
  insertFromS3,
  insertFromS3Json,
} from '../utils/clickhouse';
import { getCsvHeaders, validateJsonFile } from '../utils/file-validators';

/**
 * Get the first field of a json object for grouping
 */
function getFirstField(json: any): string {
  let obj: any;
  if (Array.isArray(json) && json.length > 0) {
    obj = json[0];
  } else {
    obj = json;
  }
  const fields = Object.keys(obj);
  return fields[0];
}

/**
 * Climate-specific file processor that handles JSON and CSV files
 * for insertion into ClickHouse
 */
export class ClimateDataProcessor implements FileProcessor {
  private minioConfig = getConfig().minio;

  /**
   * Determines if this processor can handle the given file
   */
  canProcess(file: string, mimeType: string): boolean {
    const ext = file.split('.').pop()?.toLowerCase();

    // Skip training/history/future files which are handled separately
    if (/(training|history|future)\.csv/.test(file)) {
      return false;
    }

    return ['csv', 'json'].includes(ext || '');
  }

  /**
   * Processes the file by inserting data into ClickHouse
   */
  async process(ctx: ProcessorContext): Promise<void> {
    const { bucket, file, buffer } = ctx;
    const extension = file.split('.').pop()?.toLowerCase();
    const tableName = `${bucket}_predictions`;

    const { endPoint, port, accessKey, secretKey } = this.minioConfig;
    const minioUrl = `http://${endPoint}:${port}/${bucket}/${file}`;

    logger.info(`Processing ${file} from bucket ${bucket}`);

    try {
      if (extension === 'json' && validateJsonFile(buffer)) {
        logger.debug(`Inserting JSON file ${file} into ClickHouse table ${tableName}`);

        const groupByColumnName = getFirstField(JSON.parse(buffer.toString()));

        // Create table from json
        await createTableFromJson(
          minioUrl,
          { accessKey, secretKey },
          tableName,
          groupByColumnName
        );

        // Insert data into clickhouse
        await insertFromS3Json(tableName, minioUrl, {
          accessKey,
          secretKey,
        });

        logger.info(`Successfully inserted JSON data into ${tableName}`);
      } else if (extension === 'csv' && getCsvHeaders(buffer)) {
        logger.debug(`Inserting CSV file ${file} into ClickHouse table ${tableName}`);

        // Create table
        await createTable(minioUrl, { accessKey, secretKey }, tableName);

        // Insert data into clickhouse
        await insertFromS3(tableName, minioUrl, {
          accessKey,
          secretKey,
        });

        logger.info(`Successfully inserted CSV data into ${tableName}`);
      } else {
        logger.warn(`Unknown or invalid file type - ${extension}`);
      }
    } catch (error) {
      logger.error(`Error processing file ${file}: ${error}`);
      throw error;
    }
  }
}
