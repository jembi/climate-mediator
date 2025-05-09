import axios from 'axios';
import _ from 'lodash';
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

/**
 * Formats the year and day of the year into YYYYMM format.
 *
 * @param {number} year - The year (e.g., 2012).
 * @param {number} dayOfYear - The day of the year (e.g., 200).
 * @returns {string} - The formatted date in YYYYMM format (e.g., 201207). Returns NaN on error.
 */
export function formatYearAndDay(year: number, dayOfYear: number): string {
  // Check for invalid input
  if (
    typeof year !== 'number' ||
    typeof dayOfYear !== 'number' ||
    isNaN(year) ||
    isNaN(dayOfYear) ||
    year < 1 ||
    dayOfYear < 1 ||
    dayOfYear > 366
  ) {
    //handles leap years
    throw new Error('Invalid input: year and dayOfYear must be positive numbers.');
  }

  // Calculate the month.  We'll use a simplified approach, assuming
  // 366 days max.  For more accurate calculations, especially with
  // leap years, you'd need a more complex algorithm.
  let month = Math.floor((dayOfYear - 1) / 30) + 1; // Simplified month calculation.

  // Adjust for months with fewer than 31 days.
  if (dayOfYear <= 31) {
    month = 1;
  } else if (dayOfYear <= 59) {
    //Up to end of Feb in non-leap year
    month = 2;
  } else if (dayOfYear <= 90) {
    //Up to end of March
    month = 3;
  } else if (dayOfYear <= 120) {
    month = 4;
  } else if (dayOfYear <= 151) {
    month = 5;
  } else if (dayOfYear <= 181) {
    month = 6;
  } else if (dayOfYear <= 212) {
    month = 7;
  } else if (dayOfYear <= 243) {
    month = 8;
  } else if (dayOfYear <= 273) {
    month = 9;
  } else if (dayOfYear <= 304) {
    month = 10;
  } else if (dayOfYear <= 334) {
    month = 11;
  } else {
    month = 12;
  }

  // Ensure month is two digits
  const monthString = month.toString().padStart(2, '0');

  // Combine year and month.
  const result = year.toString() + monthString;
  return result;
}

export function mergeObjectsByOuPe(
  data: Array<{ organizational_unit: string; period: string; value: number }>
) {
  const res = _.chain(data)
    .groupBy((item: { organizational_unit: string; period: string; value: number }) => 
      `${item.organizational_unit}-${item.period}`) // Group by 'ou' and 'pe'
    .map((group: Array<{ organizational_unit: string; period: string; value: number }>) =>
      _.mergeWith({}, ...group, (objValue: any, srcValue: any) => {
        // Customizer function to sum 'value' properties.
        if (_.isNumber(objValue) && _.isNumber(srcValue)) {
          return objValue + srcValue;
        }
        return srcValue; // Default behavior: overwrite other properties.
      })
    )
    // @ts-ignore
    .values() // Convert the resulting object back to an array
    .value(); // Resolve the lodash chain

  return res;
}
