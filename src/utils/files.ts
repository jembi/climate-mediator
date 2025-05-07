import axios from 'axios';
import logger from '../logger';

export async function downloadFileFromUrl(url: string): Promise<ArrayBuffer> {
  logger.info(`Downloading file from ${url}`);

  try {
    const response = await axios({
      method: 'get',
      url,
      responseType: 'arraybuffer',
    });

    logger.info(`File downloaded successfully from ${url}`);

    return response.data;
  } catch (err) {
    logger.error(`Failed to download file from ${url}: ${err}`);
    throw err;
  }
}

export function validateUrl(url: string) {
  try {
    new URL(url);
    return true;
  } catch (err) {
    return false;
  }
}
