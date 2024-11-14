import express from 'express';
import multer from 'multer';
import { getConfig } from '../config/config';
import { getCsvHeaders } from '../utils/file-validators';
import { createTable } from '../utils/clickhouse';
import logger from '../logger';

const routes = express.Router();

const bodySizeLimit = getConfig().bodySizeLimit;
const jsonBodyParser = express.json({
  type: 'application/json',
  limit: bodySizeLimit,
});

const upload = multer({ storage: multer.memoryStorage() });

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

  const tableCreated = await createTable(headers, bucket as string);

  if (!tableCreated) {
    return res
      .status(500)
      .send('Failed to create table, please check csv or use another name for the bucket');
  }

  return res.status(201).send('File uploaded successfully');
});

export default routes;
