import { createClient } from '@clickhouse/client';
import axios from 'axios';
import express from 'express';
import FormData from 'form-data';
import fs from 'fs/promises';
import multer from 'multer';
import { getConfig } from '../config/config';
import { saveToTmp } from '../helpers/files';
import {
  createErrorResponse,
  createSuccessResponse,
  UploadResponse,
} from '../helpers/responses';
import logger from '../logger';
import removePrefixMiddleWare from '../middleware/remove-prefix';
import { registerBucket } from '../openhim/openhim';
import { ModelPredictionUsingChap } from '../services/ModelPredictionUsingChap';
import { buildChapPayload } from '../utils/chap';
import {
  fetchHistoricalDisease,
  fetchOrganizations,
  fetchPopulationData,
  insertHistoricDiseaseData,
  insertOrganizationIntoTable,
  insertPopulationData,
} from '../utils/clickhouse';
import {
  extractHistoricData,
  extractPopulationData,
  validateBucketName,
} from '../utils/file-validators';
import {
  BucketDoesNotExistError,
  downloadFileAndUpload,
  ensureBucketExists,
  minioListenerHandler,
  sanitizeBucketName,
  uploadFileBufferToMinio,
  uploadToMinio,
} from '../utils/minioClient';

// Constants
const VALID_MIME_TYPES = ['text/csv', 'application/json'] as const;
type ValidMimeType = (typeof VALID_MIME_TYPES)[number];

const routes = express.Router();
routes.use(removePrefixMiddleWare('/climate'));

const bodySizeLimit = getConfig().bodySizeLimit;
const upload = multer({
  storage: multer.memoryStorage(),
  fileFilter: (_, file, cb) => {
    cb(null, VALID_MIME_TYPES.includes(file.mimetype as ValidMimeType));
  },
});

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
  bucket: string
): Promise<UploadResponse> => {
  try {
    for (const file of files) {
      const fileUrl = await saveToTmp(file.buffer, file.originalname);
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

const handleJsonPayload = async (
  file: Express.Multer.File,
  json: Object,
  bucket: string
): Promise<UploadResponse> => {
  try {
    const uploadResult = await uploadFileBufferToMinio(
      Buffer.from(JSON.stringify(json)),
      file.originalname,
      bucket,
      file.mimetype
    );

    await insertOrganizationIntoTable(file.buffer.toString());

    return uploadResult.success
      ? createSuccessResponse('UPLOAD_SUCCESS', uploadResult.message)
      : createErrorResponse('UPLOAD_FAILED', uploadResult.message);
  } catch (err) {
    logger.error('Error uploading JSON file:', err);
    throw err;
  }
};

// Main route handler

//TODO: What is the behavior if multiple files of the same name are uploaded?
routes.post('/synthetic-predict', async (req, res) => {
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
          createErrorResponse('UPLOAD_FAILED', 'Request Exceeds the Maximum Count of Files')
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

      await ensureBucketExists(bucket, createBucketIfNotExists);
      getPrediction(trainingFileFormData, historicFutureFormData, bucket);

      if (createBucketIfNotExists && getConfig().runningMode !== 'testing') {
        await registerBucket(bucket);
      }

      let response: UploadResponse;
      if (trainingFile.mimetype === 'text/csv') {
        response = await handleCsvFile([trainingFile, historicFile, futureFile], bucket);
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

async function getPrediction(
  trainingFileFormData: FormData,
  historicFutureFormData: FormData,
  bucket: string
) {
  try {
    const { chapCliApiUrl: chapApiUrl } = getConfig();
    const { url, password } = getConfig().clickhouse;
    const client = createClient({
      url,
      password,
    });

    const trainingResults = await axios.post(chapApiUrl + '/train', trainingFileFormData, {
      headers: {
        ...trainingFileFormData.getHeaders(),
      },
    });

    logger.debug(
      `CHAP Training Results: ${trainingResults.status === 201 ? 'Upload Successful' : 'Upload Failed'}`
    );

    const prediction = await axios.post(chapApiUrl + '/predict', historicFutureFormData, {
      headers: {
        ...historicFutureFormData.getHeaders(),
      },
    });

    logger.debug(
      `CHAP Prediction Results: ${prediction.status === 201 ? 'Successful Received Prediction' : 'Failed to Received Prediction'}`
    );
    const { predictions } = prediction.data;
    const stringifiedPrediction = JSON.stringify(predictions);
    const originalFileName = `prediction-result.json`;
    const fileUrl = await saveToTmp(Buffer.from(stringifiedPrediction), originalFileName);

    await uploadToMinio(fileUrl, originalFileName, bucket, 'application/json');
    await fs.unlink(fileUrl);
  } catch (error) {
    logger.error(`Failed to receive prediction: ${error}`);
  }
}

routes.post('/predict', upload.single('file'), async (req, res) => {
  try {
    const file = req.file;
    const chapUrl = process.env.CHAP_URL;

    if (!chapUrl) {
      logger.error('Chap URL not set');
      return res.status(500).json(createErrorResponse('ENV_MISSING', 'Chap URL not set'));
    }

    if (!file) {
      logger.debug('No file uploaded');
      return res.status(400).json(createErrorResponse('FILE_MISSING', 'No file uploaded'));
    }

    try {
      const historicData = extractHistoricData(file.buffer.toString());
      await insertHistoricDiseaseData(historicData);
      const populationData = extractPopulationData(file.buffer.toString());
      await insertPopulationData(populationData);
    } catch (error) {
      logger.error('There was an issue inserting the Historic Data: ' + JSON.stringify(error));
    }

    const modelPrediction = new ModelPredictionUsingChap(chapUrl, logger);

    // start the Chap prediction job
    const predictResponse = await modelPrediction.predict({ data: file.buffer.toString() });

    if (predictResponse?.status === 'success') {
      // wait for the prediction job to finish
      const predictionResults = (await new Promise((resolve, reject) => {
        const interval = setInterval(async () => {
          const statusResponse = await modelPrediction.getStatus();
          if (statusResponse?.status === 'idle' && statusResponse?.ready) {
            clearInterval(interval);
            resolve((await modelPrediction.getResult()).data);
          }
        }, 250);
      })) as any;

      // get organization code
      const orgCode = JSON.parse(file.buffer.toString())?.orgUnitsGeoJson.features[0]
        .properties.code;

      const bucketName = sanitizeBucketName(`${file.originalname.split('.')[0]}`);

      const predictionResultsForMinio = predictionResults?.dataValues?.map((d: any) => {
        return {
          ...d,
          orgCode: orgCode ?? undefined,
          diseaseId: predictionResults.diseaseId as string,
        };
      });

      await ensureBucketExists(bucketName, true);

      await handleJsonPayload(file, predictionResultsForMinio, bucketName);

      return res.status(200).json(predictionResultsForMinio);
    }

    return res
      .status(500)
      .json({ error: 'Error predicting model. Error response from Chap API' });
  } catch (err) {
    logger.error('Error predicting model:');
    logger.error(err);
    return res.status(500).json({ error: 'An unexpected error has occured' });
  }
});

routes.get('/predict-inverse', async (req, res) => {
  try {
    const chapUrl = process.env.CHAP_URL;

    if (!chapUrl) {
      logger.error('Chap URL not set');
      return res.status(500).json(createErrorResponse('ENV_MISSING', 'Chap URL not set'));
    }

    const organizations = await fetchOrganizations();
    const historicDisease = await fetchHistoricalDisease();
    const populations = await fetchPopulationData();

    if (!organizations.length || !historicDisease.length || !populations.length) {
      return res.status(500).json({ error: 'No data found in Clickhouse' });
    }

    const payload = buildChapPayload(historicDisease, organizations, populations);

    const modelPrediction = new ModelPredictionUsingChap(chapUrl, logger);

    // start the Chap prediction job
    const predictResponse = await modelPrediction.predict({ data: payload });

    if (predictResponse?.status === 'success') {
      // wait for the prediction job to finish
      const predictionResults = (await new Promise((resolve, reject) => {
        const interval = setInterval(async () => {
          const statusResponse = await modelPrediction.getStatus();
          if (statusResponse?.status === 'idle' && statusResponse?.ready) {
            clearInterval(interval);
            resolve((await modelPrediction.getResult()).data);
          }
        }, 333);
      })) as any;

      const bucketName = sanitizeBucketName('prediction');
      const fileName = new Date().getTime() + '.json';
      const predictionResultsForMinio = predictionResults?.dataValues?.map((d: any) => {
        return {
          ...d,
          diseaseId: predictionResults.diseaseId as string,
        };
      });

      await ensureBucketExists(bucketName, true);
      const uploadResult = await uploadFileBufferToMinio(
        Buffer.from(JSON.stringify(predictionResultsForMinio)),
        fileName,
        bucketName,
        'application/json'
      );

      if (uploadResult.success) {
        return res.status(200).json({
          message: 'Prediction results uploaded successfully',
          results: predictionResultsForMinio,
        });
      } else {
        return res.status(500).json({ message: 'Failed to upload prediction results' });
      }
    }

    return res
      .status(500)
      .json({ error: 'Error predicting model. Error response from Chap API' });
  } catch (err) {
    logger.error('Error predicting model:');
    logger.error(err);
    return res.status(500).json({ error: 'An unexpected error has occured' });
  }
});

routes.get('/process-climate-data', async (req, res) => {
  try {
    const bucket = req.query.bucket as string;
    const file = req.query.file as string;
    const tableName = req.query.tableName as string;

    await minioListenerHandler(bucket, file, tableName);

    return res.status(200).json({ bucket, file, tableName, clickhouseInsert: 'Success' });
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

    const bucket = req.query.bucket as string | undefined;
    const response = await downloadFileAndUpload(bucket);
    
    if (response?.error && response.error === 'No buckets configured in OpenHIM config') {
      return res.status(400).json(createErrorResponse('BUCKET_MISSING', response.error));
    }

    return res.status(200).json({ download: 'success', upload: 'success' });
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

routes.post('/upload', upload.single('file'), async (req, res) => {
  try {
    const file = req.file;
    const bucket = req.query.bucket as string;
    const region = req.query.region as string;
    const createBucketIfNotExists = req.query.createBucketIfNotExists === 'true';

    if (!file) {
      logger.debug('No file uploaded');
      return res.status(400).json(createErrorResponse('FILE_MISSING', 'No file uploaded'));
    }

    if (!bucket) {
      logger.debug('No bucket provided');
      return res.status(400).json(createErrorResponse('BUCKET_MISSING', 'No bucket provided'));
    }

    if (!validateBucketName(bucket)) {
      logger.debug(`Invalid bucket name ${bucket}`);
      return res
        .status(400)
        .json(
          createErrorResponse(
            'INVALID_BUCKET_NAME',
            'Bucket names must be between 3 (min) and 63 (max) characters long. Can consist only of lowercase letters, numbers, dots (.), and hyphens (-). Must not start with the prefix xn--. Must not end with the suffix -s3alias. This suffix is reserved for access point alias names.'
          )
        );
    }

    await ensureBucketExists(bucket, createBucketIfNotExists);

    const response =
      file.mimetype === 'text/csv'
        ? await handleCsvFile([file], bucket)
        : await handleJsonFile(file, bucket, region);

    if (createBucketIfNotExists && getConfig().runningMode !== 'testing') {
      await registerBucket(bucket);
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

export default routes;
