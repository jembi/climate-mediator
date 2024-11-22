import express from 'express';
import multer from 'multer';
import { getConfig } from '../config/config';
import { getCsvHeaders } from '../utils/file-validators';
import logger from '../logger';
import fs from 'fs';
import path from 'path';
import { uploadToMinio } from '../utils/minioClient';
import e from 'express';
const routes = express.Router();

const bodySizeLimit = getConfig().bodySizeLimit;
const jsonBodyParser = express.json({
  type: 'application/json',
  limit: bodySizeLimit,
});

const upload = multer({ storage: multer.memoryStorage() });

const saveCsvToTmp = (fileBuffer: Buffer, fileName: string): string => {
  const tmpDir = path.join(process.cwd(), 'tmp');
  
  // Create tmp directory if it doesn't exist
  if (!fs.existsSync(tmpDir)) {
    fs.mkdirSync(tmpDir);
  }
  
  const fileUrl = path.join(tmpDir, fileName);
  fs.writeFileSync(fileUrl, fileBuffer);
  logger.info(`fileUrl: ${fileUrl}`);
  
  return fileUrl;
};

const isValidFileType = (file: Express.Multer.File): boolean => {
  const validMimeTypes = ['text/csv', 'application/json'];
  return validMimeTypes.includes(file.mimetype);
};

function validateJsonFile(buffer: Buffer): boolean {
  try {
    JSON.parse(buffer.toString());
    return true;
  } catch {
    return false;
  }
}

routes.post('/upload', upload.single('file'), async (req, res) => {
  const file = req.file;
  const bucket = req.query.bucket;

  if (!file) {
    logger.error('No file uploaded');
    return res.status(400).json({
      status: 'error',
      code: 'FILE_MISSING',
      message: 'No file uploaded'
    });
  }

  if (!bucket) {
    logger.error('No bucket provided');
    return res.status(400).json({
      status: 'error',
      code: 'BUCKET_MISSING',
      message: 'No bucket provided'
    });
  }

  if (!isValidFileType(file)) {
    logger.error(`Invalid file type: ${file.mimetype}`);
    return res.status(400).json({
      status: 'error',
      code: 'INVALID_FILE_TYPE',
      message: 'Invalid file type. Please upload either a CSV or JSON file'
    });
  }

  // For CSV files, validate headers
  if (file.mimetype === 'text/csv') {
    const headers = getCsvHeaders(file.buffer);
    if (!headers) {
      return res.status(400).json({
        status: 'error',
        code: 'INVALID_CSV_FORMAT',
        message: 'Invalid CSV file format'
      });
    }
    const fileUrl = saveCsvToTmp(file.buffer, file.originalname);
    try {
      const uploadResult = await uploadToMinio(fileUrl, file.originalname, bucket as string, file.mimetype);
      // Clean up the temporary file
      fs.unlinkSync(fileUrl);

      if (uploadResult.success) {
        return res.status(201).json({
          status: 'success',
          code: 'UPLOAD_SUCCESS',
          message: uploadResult.message
        });
      } else {
        return res.status(400).json({
          status: 'error',
          code: 'UPLOAD_FAILED',
          message: uploadResult.message
        });
      }
    } catch (error: unknown) {
      logger.error('Error uploading file to Minio:', error);
      return res.status(500).json({
        status: 'error',
        code: 'INTERNAL_SERVER_ERROR',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  } else if (file.mimetype === 'application/json') {
    if (!validateJsonFile(file.buffer)) {
      return res.status(400).json({
        status: 'error',
        code: 'INVALID_JSON_FORMAT',
        message: 'Invalid JSON file format'
      });
    }

    return res.status(200).json({
      status: 'success',
      code: 'JSON_VALID',
      message: 'JSON file is valid - Future implementation'
    });
  }
});

export default routes;
