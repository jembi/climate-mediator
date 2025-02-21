import express from 'express';
import multer from 'multer';
import { getConfig } from '../config/config';
import { getCsvHeaders, validateBucketName } from '../utils/file-validators';
import logger from '../logger';
import fs from 'fs/promises';
import path from 'path';
import {
  BucketDoesNotExistError,
  ensureBucketExists,
  uploadToMinio,
} from '../utils/minioClient';
import { registerBucket } from '../openhim/openhim';
import axios from 'axios';
import FormData from 'form-data'
import { createClient } from '@clickhouse/client';

// Constants
const VALID_MIME_TYPES = ['text/csv', 'application/json'] as const;
type ValidMimeType = (typeof VALID_MIME_TYPES)[number];

interface UploadResponse {
  status: 'success' | 'error';
  code: string;
  message: string;
}

const routes = express.Router();
const bodySizeLimit = getConfig().bodySizeLimit;
const upload = multer({
  storage: multer.memoryStorage(),
  fileFilter: (_, file, cb) => {
    cb(null, VALID_MIME_TYPES.includes(file.mimetype as ValidMimeType));
  },
});

// Helper functions
const createErrorResponse = (code: string, message: string): UploadResponse => ({
  status: 'error',
  code,
  message,
});

const createSuccessResponse = (code: string, message: string): UploadResponse => ({
  status: 'success',
  code,
  message,
});

const saveCsvToTmp = async (fileBuffer: Buffer, fileName: string): Promise<string> => {
  const tmpDir = path.join(process.cwd(), 'tmp');
  await fs.mkdir(tmpDir, { recursive: true });

  const fileUrl = path.join(tmpDir, fileName);
  await fs.writeFile(fileUrl, fileBuffer);
  logger.info(`File saved: ${fileUrl}`);

  return fileUrl;
};

const validateJsonFile = (buffer: Buffer): boolean => {
  try {
    JSON.parse(buffer.toString());
    return true;
  } catch {
    return false;
  }
};

// File handlers
const handleCsvFile = async (
  files: Express.Multer.File[],
  bucket: string,
  region: string
): Promise<UploadResponse> => {
  try {
    for (const file of files) {
      const fileUrl = await saveCsvToTmp(file.buffer, file.originalname);
      const uploadResult = await uploadToMinio(
        fileUrl,
        file.originalname,
        bucket,
        file.mimetype
      );
      await fs.unlink(fileUrl);
      logger.debug(`Upload Successful: ${uploadResult.message}`);
    }

    return createSuccessResponse(
      'SUCCESSFULLY_UPLOADED_FILES',
      'All files uploaded successfully'
    );
  } catch (error) {
    return createErrorResponse('FAILED_TO_UPLOAD_FILES', 'There was an issue uploading files');
  }
};

const handleJsonFile = (file: Express.Multer.File): UploadResponse => {
  if (!validateJsonFile(file.buffer)) {
    return createErrorResponse('INVALID_JSON_FORMAT', 'Invalid JSON file format');
  }
  return createSuccessResponse('JSON_VALID', 'JSON file is valid - Future implementation');
};

// Main route handler

//TODO: What is the behavior if multiple files of the same name are uploaded?
routes.post('/upload', async (req, res) => {
  const handleUpload = upload.fields([
    { name: 'training', maxCount: 1 },
    { name: 'historic', maxCount: 1 },
    { name: 'future', maxCount: 1 },
  ]);

  handleUpload(req, res, async (err) => {
    const error = err as multer.MulterError;

    // handle error if they exceed the max count
    if (error !== undefined && error.code === 'LIMIT_FILE_COUNT') {
      logger.error(`Error uploading files: ${err}`);
      return res
        .status(500)
        .json(
          createErrorResponse(
            'UPLOAD_FAILED',
            'Unexpected Field Provided When Uploading Files'
          )
        );
    }

    // handle error if unknown file is provided
    if (error !== undefined && error.code === 'LIMIT_UNEXPECTED_FILE') {
      logger.error(`Error uploading files: ${err}`);
      return res
        .status(500)
        .json(
          createErrorResponse(
            'UPLOAD_FAILED',
            'Unexpected Field Provided When Uploading Files'
          )
        );
    }

    try {
      //@ts-ignore
      const trainingFile = req.files?.training?.[0] as Express.Multer.File;
      //@ts-ignore
      const historicFile = req.files?.historic?.[0] as Express.Multer.File;
      //@ts-ignore
      const futureFile = req.files?.future?.[0] as Express.Multer.File;

      if (!trainingFile || !historicFile || !futureFile) {
        logger.error('Missing files');
        return res.status(400).json(createErrorResponse('FILE_MISSING', 'Missing files'));
      }

      const bucket = req.query.bucket as string;
      const region = req.query.region as string;
      const createBucketIfNotExists = req.query.createBucketIfNotExists === 'true';

      if (!bucket) {
        logger.error('No bucket provided');
        return res
          .status(400)
          .json(createErrorResponse('BUCKET_MISSING', 'No bucket provided'));
      }

      if (!validateBucketName(bucket)) {
        logger.error(`Invalid bucket name ${bucket}`);
        return res
          .status(400)
          .json(
            createErrorResponse(
              'INVALID_BUCKET_NAME',
              'Bucket names must be between 3 (min) and 63 (max) characters long. Can consist only of lowercase letters, numbers, dots (.), and hyphens (-). Must not start with the prefix xn--. Must not end with the suffix -s3alias. This suffix is reserved for access point alias names.'
            )
          );
      }

      const trainingFileFormData = new FormData();
      trainingFileFormData.append('training_data', trainingFile.buffer, {
        filename: trainingFile.originalname,
        contentType: trainingFile.mimetype,
      });
      const historicFutureFormData = new FormData();
      historicFutureFormData.append('historic_data', historicFile.buffer, {
        filename: historicFile.originalname,
        contentType: historicFile.mimetype,
      });
      historicFutureFormData.append('future_data', futureFile.buffer, {
        filename: futureFile.originalname,
        contentType: futureFile.mimetype,
      });

      getPrediction(trainingFileFormData, historicFutureFormData, bucket);

      await ensureBucketExists(bucket, region, createBucketIfNotExists);

      if (createBucketIfNotExists && getConfig().runningMode !== 'testing') {
        await registerBucket(bucket, region);
      }

      let response: UploadResponse;
      if (trainingFile.mimetype === 'text/csv') {
        response = await handleCsvFile(
          [trainingFile, historicFile, futureFile],
          bucket,
          region
        );
      } else {
        response = createErrorResponse('INVALID_FILE_TYPE', 'Invalid file type');
      }

      const statusCode = response.status === 'success' ? 201 : 400;
      return res.status(statusCode).json(response);
    } catch (e) {
      logger.error('Error processing upload:', JSON.stringify(e));
      if (e instanceof BucketDoesNotExistError) {
        const error = e as BucketDoesNotExistError;
        return res
          .status(400)
          .json(createErrorResponse('BUCKET_DOES_NOT_EXIST', error.message));
      }
      return res
        .status(500)
        .json(
          createErrorResponse(
            'INTERNAL_SERVER_ERROR',
            e instanceof Error ? e.message : 'Unknown error'
          )
        );
    }
  });
});

async function getPrediction(trainingFileFormData: FormData, historicFutureFormData: FormData, bucket: string) {
  try {
    const { chapApiUrl } = getConfig();
    const { url, password } = getConfig().clickhouse;
    const client = createClient({
        url,
        password,
      });

    const  trainingResults = await axios.post(chapApiUrl + '/train', trainingFileFormData, {
      headers: {
        ...trainingFileFormData.getHeaders()
      },
    })

    logger.debug(`CHAP Training Results: ${trainingResults.status === 201 ? 'Upload Successful':'Upload Failed'}`)

    const prediction = await axios.post(chapApiUrl + '/predict', historicFutureFormData, {
      headers: {
        ...historicFutureFormData.getHeaders()
      },
    })
    
    logger.debug(`CHAP Prediction Results: ${prediction.status === 201 ? 'Successful Received Prediction':'Failed to Received Prediction'}`);

    const stringifiedPrediction = JSON.stringify(prediction.data);
    const originalFileName = 'prediction-result.json';
    const fileUrl = await saveCsvToTmp(Buffer.from(stringifiedPrediction), originalFileName);

    await uploadToMinio(
      fileUrl,
      originalFileName,
      bucket,
      'application/json'
    );
    await fs.unlink(fileUrl);

    //TODO: check if table does not exist
    await client.exec({query: `CREATE TABLE ${bucket}_prediction (time_period String, predicted_value Float64) ENGINE = MergeTree ORDER BY (time_period);`});

    await client.insert({
      table: `${bucket}_prediction`,
      values: prediction.data.predictions,
      format: 'JSONEachRow'
    });

    logger.debug('Clickhouse Data Insertion Completed')

  } catch (error) {
    logger.error(`Failed to receive prediction: ${error}`);
  }
}

export default routes;
