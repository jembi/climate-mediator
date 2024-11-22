import * as Minio from "minio";
import { getConfig } from '../config/config';
import logger from '../logger';

const {endPoint, port, useSSL, bucketRegion, accessKey, secretKey} = getConfig().minio;

// Create a shared Minio client instance
const minioClient = new Minio.Client({
    endPoint,
    port,
    useSSL,
    accessKey,
    secretKey
});

interface MinioResponse {
    success: boolean;
    message: string;
}

interface FileExistsResponse extends MinioResponse {
    exists: boolean;
}

/**
 * Ensures a bucket exists, creates it if it doesn't
 * @param {string} bucket - Bucket name
 * @returns {Promise<void>}
 */
async function ensureBucketExists(bucket: string): Promise<void> {
    const exists = await minioClient.bucketExists(bucket);
    if (!exists) {
        await minioClient.makeBucket(bucket, bucketRegion);
        logger.info(`Bucket ${bucket} created in "${bucketRegion}"`);
    }
}

/**
 * Checks if a file exists in the specified Minio bucket
 * @param {string} fileName - Name of the file to check
 * @param {string} bucket - Bucket name
 * @param {string} fileType - Expected file type
 * @returns {Promise<FileExistsResponse>}
 */
export async function checkFileExists(
    fileName: string, 
    bucket: string, 
    fileType: string
): Promise<FileExistsResponse> {
    try {
        const bucketExists = await minioClient.bucketExists(bucket);
        if (!bucketExists) {
            return {
                exists: false,
                success: false,
                message: `Bucket ${bucket} does not exist`
            };
        }

        const stats = await minioClient.statObject(bucket, fileName);
        const exists = stats.metaData?.['content-type'] === fileType;
        
        return {
            exists,
            success: true,
            message: exists 
                ? `File ${fileName} exists in bucket ${bucket}`
                : `File ${fileName} does not exist in bucket ${bucket}`
        };
    } catch (err) {
        const error = err as Error;
        if ((error as any).code === 'NotFound') {
            return {
                exists: false,
                success: true,
                message: `File ${fileName} not found in bucket ${bucket}`
            };
        }
        
        logger.error('Error checking file existence:', error);
        return {
            exists: false,
            success: false,
            message: `Error checking file existence: ${error.message}`
        };
    }
}

/**
 * Uploads a file to Minio storage
 * @param {string} sourceFile - Path to the file to upload
 * @param {string} destinationObject - Name for the uploaded object
 * @param {string} bucket - Bucket name
 * @param {string} fileType - Type of file being uploaded
 * @param {Object} [customMetadata={}] - Optional custom metadata
 * @returns {Promise<MinioResponse>}
 */
export async function uploadToMinio(
    sourceFile: string,
    destinationObject: string,
    bucket: string,
    fileType: string,
    customMetadata = {}
): Promise<MinioResponse> {
    try {
        await ensureBucketExists(bucket);

        const fileCheck = await checkFileExists(destinationObject, bucket, fileType);
        if (fileCheck.exists) {
            return { 
                success: false, 
                message: fileCheck.message 
            };
        }

        const metaData = {
            'Content-Type': fileType,
            'X-Upload-Id': crypto.randomUUID(),
            ...customMetadata
        };
    
        await minioClient.fPutObject(bucket, destinationObject, sourceFile, metaData);
        const successMessage = `File ${sourceFile} uploaded as object ${destinationObject} in bucket ${bucket}`;
        logger.info(successMessage);
        
        return {
            success: true, 
            message: successMessage
        };
    } catch (error) {
        const errorMessage = `Error uploading file: ${error instanceof Error ? error.message : String(error)}`;
        logger.error(errorMessage);
        return {
            success: false,
            message: errorMessage
        };
    }   
}
