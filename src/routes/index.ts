import express from 'express';
import multer from 'multer';
import { getConfig } from '../config/config';
import { getCsvHeaders, validateBucketName } from '../utils/file-validators';
import logger from '../logger';
import fs from 'fs/promises';
import path from 'path';
import {
  BucketDoesNotExistError,
  downloadFileAndUpload,
  ensureBucketExists,
  minioListenerHandler,
  uploadToMinio,
} from '../utils/minioClient';
import { registerBucket } from '../openhim/openhim';

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

routes.get('/process-climate-data', async (req, res) => {
  try {
    const bucket = req.query.bucket as string;
    const file = req.query.file as string;
    const tableName = req.query.tableName as string;

    await minioListenerHandler(bucket, file, tableName);

    return res.status(200).json({bucket, file, tableName, clickhouseInsert: 'Success'});
  } catch (e) {
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

routes.get('/download-climate-data', async (req, res) => {
  try {
    logger.info('Downloading and uploading of climate data started');

    const bucket = req.query.bucket;

    await downloadFileAndUpload(bucket as string);

    return res.status(200).json({download: 'success', upload: 'success'});
  } catch (e) {
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

export default routes;
