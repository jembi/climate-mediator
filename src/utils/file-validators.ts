import { PredictionPayload } from "./prediction-payload";

export interface HistoricData{
  organizational_unit: string;
  period: string;
  value: number;
}

export interface PopulationData{
  organizational_unit: string;
  period: string;
  value: number;
}

export function validateJsonFile(file: Buffer) {
  const json = file.toString();
  try {
    JSON.parse(json);
  } catch (e) {
    return false;
  }
  return true;
}

export function getCsvHeaders(file: Buffer) {
  //convert the buffer to a string
  const csv = file.toString();
  //check if the new line character is \n or \r\n
  const newLineChar = csv.includes('\r\n') ? '\r\n' : '\n';
  //get the first line of the csv file
  const firstLine = csv.split(newLineChar)[0];
  //split the first line by commas
  const columns = firstLine.split(',');

  if (columns.length === 0) return false;

  return columns;
}

export function validateBucketName(bucket: string): boolean {
  // Bucket names must be between 3 (min) and 63 (max) characters long.
  // Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
  // Bucket names must not start with the prefix xn--.
  // Bucket names must not end with the suffix -s3alias. This suffix is reserved for access point alias names.
  const regex = new RegExp(/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/);
  return regex.test(bucket);
}

export function extractHistoricData(jsonStringified: string): HistoricData[]{
  const jsonPayload = JSON.parse(jsonStringified) as PredictionPayload;
  const diseaseCases = jsonPayload.features.find(feature => feature['featureId'] === 'disease_cases')
  
  if(diseaseCases === undefined){
    throw new Error("Could not find historic disease data within payload");
  }

  const {data} = diseaseCases;
  const historicData = data.map((datum: {ou: string, pe: string, value: number}) => ({
    organizational_unit: datum.ou,
    period: datum.pe,
    value: datum.value,
  }));
  return historicData;
}

export function extractPopulationData(jsonStringified: string): PopulationData[]{
  const jsonPayload = JSON.parse(jsonStringified) as PredictionPayload;
  const populationDatas = jsonPayload.features.find(feature => feature['featureId'] === 'population')

  if(populationDatas === undefined){ 
    throw new Error("Could not find population data within payload");
  }

  const {data} = populationDatas;
  const populationData = data.map((datum: {ou: string, pe: string, value: number}) => ({
    organizational_unit: datum.ou,
    period: datum.pe,
    value: datum.value,
  }));
  return populationData;
}
