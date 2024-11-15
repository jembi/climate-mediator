import express from 'express';
import multer from 'multer';
import { getConfig } from '../config/config';
import { getCsvHeaders } from '../utils/file-validators';
import logger from '../logger';
import fs from 'fs';
import path from 'path';
import { uploadToMinio } from '../utils/minioClient';
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

routes.post('/upload', upload.single('file'), async (req, res) => {
  const file = req.file;
  const bucket = req.query.bucket;

  if (!file) {
    logger.error('No file uploaded');
    return res.status(400).send('No file uploaded');
  }

  if (!bucket) {
    logger.error('No bucket provided');
    return res.status(400).send('No bucket provided');
  }

  const headers = getCsvHeaders(file.buffer);

  if (!headers) {
    return res.status(400).send('Invalid file type, please upload a valid CSV file');
  }
  const fileUrl = saveCsvToTmp(file.buffer, file.originalname);

  const uploadResult = await uploadToMinio(fileUrl,file.originalname, bucket as string);
  // const tableCreated = await createTable(headers, bucket as string);
  logger.info(`file created: ${file.originalname}`);

  fs.unlinkSync(fileUrl);

  return res.status(201).send('File uploaded successfully');
});

export default routes;
