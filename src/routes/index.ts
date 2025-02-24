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
  sanitizeBucketName,
  uploadToMinio,
} from '../utils/minioClient';
import { registerBucket } from '../openhim/openhim';
import { ModelPredictionUsingChap } from '../services/ModelPredictionUsingChap';

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

const saveToTmp = async (fileBuffer: Buffer, fileName: string): Promise<string> => {
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
  file: Express.Multer.File,
  bucket: string,
  region: string
): Promise<UploadResponse> => {
  const headers = getCsvHeaders(file.buffer);
  if (!headers) {
    return createErrorResponse('INVALID_CSV_FORMAT', 'Invalid CSV file format');
  }

  const fileUrl = await saveToTmp(file.buffer, file.originalname);
  try {
    const uploadResult = await uploadToMinio(
      fileUrl,
      file.originalname,
      bucket,
      file.mimetype
    );
    await fs.unlink(fileUrl);

    return uploadResult.success
      ? createSuccessResponse('UPLOAD_SUCCESS', uploadResult.message)
      : createErrorResponse('UPLOAD_FAILED', uploadResult.message);
  } catch (error) {
    logger.error('Error uploading file to Minio:', error);
    throw error;
  }
};

const handleJsonFile = async (
  file: Express.Multer.File,
  bucket: string,
  region: string
): Promise<UploadResponse> => {
  if (!validateJsonFile(file.buffer)) {
    return createErrorResponse('INVALID_JSON_FORMAT', 'Invalid JSON file format');
  }

  const jsonString = file.buffer.toString().replace(/\n/g, '').replace(/\r/g, '');
  const fileUrl = await saveToTmp(Buffer.from(jsonString), file.originalname);
  try {
    const uploadResult = await uploadToMinio(
      fileUrl,
      file.originalname,
      bucket,
      file.mimetype
    );
    await fs.unlink(fileUrl);

    return uploadResult.success
      ? createSuccessResponse('UPLOAD_SUCCESS', uploadResult.message)
      : createErrorResponse('UPLOAD_FAILED', uploadResult.message);
  } catch (error) {
    logger.error('Error uploading file to Minio:', error);
    throw error;
  }
};

const handleJsonPayload = async (file: Express.Multer.File, json: Object, bucket: string): Promise<UploadResponse> => {
  try {
    const fileUrl = await saveToTmp(Buffer.from(JSON.stringify(json)), file.originalname);

    const uploadResult = await uploadToMinio(
      fileUrl,
      file.originalname,
      bucket,
      file.mimetype
    );

    await fs.unlink(fileUrl);
   
    return uploadResult.success
      ? createSuccessResponse('UPLOAD_SUCCESS', uploadResult.message)
      : createErrorResponse('UPLOAD_FAILED', uploadResult.message);
  } catch (err) {
    logger.error('Error uploading JSON file:', err);
    throw err;
  }
  return createSuccessResponse('JSON_VALID', 'JSON file is valid - Future implementation');
};

// Main route handler
routes.post('/upload', upload.single('file'), async (req, res) => {
  try {
    const file = req.file;
    const bucket = req.query.bucket as string;
    const region = req.query.region as string;
    const createBucketIfNotExists = req.query.createBucketIfNotExists === 'true';

    if (!file) {
      logger.error('No file uploaded');
      return res.status(400).json(createErrorResponse('FILE_MISSING', 'No file uploaded'));
    }

    if (!bucket) {
      logger.error('No bucket provided');
      return res.status(400).json(createErrorResponse('BUCKET_MISSING', 'No bucket provided'));
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

    await ensureBucketExists(bucket, region, createBucketIfNotExists);

    const response =
      file.mimetype === 'text/csv'
        ? await handleCsvFile(file, bucket, region)
        : await handleJsonFile(file, bucket, region);

    if (createBucketIfNotExists && getConfig().runningMode !== 'testing') {
      await registerBucket(bucket, region);
    }

    const statusCode = response.status === 'success' ? 201 : 400;
    return res.status(statusCode).json(response);
  } catch (e) {
    logger.error('Error processing upload:', e);

    if (e instanceof BucketDoesNotExistError) {
      const error = e as BucketDoesNotExistError;
      return res.status(400).json(createErrorResponse('BUCKET_DOES_NOT_EXIST', error.message));
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

routes.post('/predict', upload.single('file'), async (req, res) => {
  try {
    const file = req.file;
    const region = process.env.MINIO_BUCKET_REGION || 'us-east-1'
    const chapUrl = process.env.CHAP_URL || 'http://localhost:8000'

    if (!chapUrl) {
      logger.error('Chap URL not set');
      return res.status(500).json(createErrorResponse('ENV_MISSING', 'Chap URL not set'));
    }

    if (!file) {
      logger.error('No file uploaded');
      return res.status(400).json(createErrorResponse('FILE_MISSING', 'No file uploaded'));
    }

    const modelPrediction = new ModelPredictionUsingChap(chapUrl, logger);

    // start the Chap prediction job
    const predictResponse = await modelPrediction.predict({ data: file.buffer.toString() });

    if (predictResponse?.status === 'success') {
      // wait for the prediction job to finish
      const predictionResults = await new Promise((resolve, reject) => {
        const interval = setInterval(async () => {
          const statusResponse = await modelPrediction.getStatus();
          if (statusResponse?.status === 'idle' && statusResponse?.ready) {
            clearInterval(interval);
            resolve((await modelPrediction.getResult()).data);
          }
        }, 250);
      }) as any;

      const bucketName = sanitizeBucketName(
        `${file.originalname.split('.')[0]}-${Math.round(new Date().getTime() / 1000)}`
      )

      const predictionResultsForMinio = predictionResults?.dataValues?.map((d: any) => {
        return {
          ...d,
          diseaseId: predictionResults.diseaseId as string,
        }
      });

      await ensureBucketExists(bucketName, region, true);

      await handleJsonPayload(file, predictionResults, bucketName);

      return res.status(200).json(predictionResultsForMinio);
    }

    return res.status(500).json({ error: 'Error predicting model. Error response from Chap API' });
  } catch (err) {
    logger.error('Error predicting model:');
    logger.error(err);
    return res.status(500).json({ error: 'An unexpected error has occured' });
  }
});

export default routes;
