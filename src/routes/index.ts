import express from 'express';
import multer from 'multer';
import { getConfig } from '../config/config';

const routes = express.Router();

const bodySizeLimit = getConfig().bodySizeLimit;
const jsonBodyParser = express.json({
  type: 'application/json',
  limit: bodySizeLimit,
});

const upload = multer({ dest: 'tmp/' });

routes.post('/upload', upload.single('file'), (req, res) => {
  res.send('File uploaded successfully');
});


export default routes;