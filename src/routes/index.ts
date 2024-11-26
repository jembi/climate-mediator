import express from 'express';
import multer from 'multer';
import { getConfig } from '../config/config';
import { getCsvHeaders } from '../utils/file-validators';
import logger from '../logger';
import fs from 'fs/promises';
import path from 'path';
import {
  BucketDoesNotExistError,
  ensureBucketExists,
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
  file: Express.Multer.File,
  bucket: string,
  region: string
): Promise<UploadResponse> => {
  const headers = getCsvHeaders(file.buffer);
  if (!headers) {
    return createErrorResponse('INVALID_CSV_FORMAT', 'Invalid CSV file format');
  }

  const fileUrl = await saveCsvToTmp(file.buffer, file.originalname);
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

const handleJsonFile = (file: Express.Multer.File): UploadResponse => {
  if (!validateJsonFile(file.buffer)) {
    return createErrorResponse('INVALID_JSON_FORMAT', 'Invalid JSON file format');
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

    await ensureBucketExists(bucket, region, createBucketIfNotExists);

    const response =
      file.mimetype === 'text/csv'
        ? await handleCsvFile(file, bucket, region)
        : handleJsonFile(file);

    createBucketIfNotExists && (await registerBucket(bucket, region));

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

export default routes;
