import * as Minio from "minio";
import { getConfig } from '../config/config';
import logger from '../logger';

const {endPoint, port, useSSL, bucketRegion, accessKey, secretKey, prefix, suffix } =
    getConfig().minio;

/**
 * Uploads a file to Minio storage
 * @param {string} sourceFile - Path to the file to upload
 * @param {string} destinationObject - Name for the uploaded object
 * @param {Object} [customMetadata={}] - Optional custom metadata
 * @returns {Promise<void>}
 */
export async function uploadToMinio(sourceFile: string, destinationObject: string, bucket: string, fileType: string, customMetadata = {}) {
    const minioClient = new Minio.Client({
        endPoint,
        port,
        useSSL,
        accessKey,
        secretKey
    });
    // Check if bucket exists, create if it doesn't
    const exists = await minioClient.bucketExists(bucket);
    if (!exists) {
        await minioClient.makeBucket(bucket, bucketRegion);
        logger.info(`Bucket ${bucket} created in "${bucketRegion}".`);
    }

    try {
        const fileCheck = await checkFileExists(destinationObject, bucket, fileType);
        if (fileCheck.exists) {
            return { success: false, message: fileCheck.message };
        } else {
            const metaData = {
                'Content-Type': fileType,
                'X-Upload-Id': crypto.randomUUID(), 
                ...customMetadata
            };
        
            // Upload the file
            await minioClient.fPutObject(bucket, destinationObject, sourceFile, metaData);
            logger.info(`File ${sourceFile} uploaded as object ${destinationObject} in bucket ${bucket}`);
            return {success: true, message: `File ${sourceFile} uploaded as object ${destinationObject} in bucket ${bucket}`};
        }
    } catch (error) {
        console.error('Error checking file:', error);
        return {success: false, message: `Error uploading file: ${error}`};
    }   
}

/**
 * Checks if a CSV file exists in the specified Minio bucket
 * @param {string} fileName - Name of the CSV file to check
 * @param {string} bucket - Bucket name
 * @returns {Promise<boolean>} - Returns true if file exists, false otherwise
 */
export async function checkFileExists(fileName: string, bucket: string, fileType: string): Promise<{ exists: boolean, message: string }> {
    const minioClient = new Minio.Client({
        endPoint,
        port,
        useSSL,
        accessKey,
        secretKey
    });

    try {
        // Check if bucket exists first
        const bucketExists = await minioClient.bucketExists(bucket);
        if (!bucketExists) {
            return {
                exists: false,
                message: `Bucket ${bucket} does not exist`
            };
        }

        // Get object stats to check if file exists
        const stats = await minioClient.statObject(bucket, fileName);
        if (stats.metaData && stats.metaData['content-type'] === fileType) {
            return {
                exists: true,
                message: `File ${fileName} exists in bucket ${bucket}`
            };
        } else {
            return {
                exists: false,
                message: `File ${fileName} does not exist in bucket ${bucket}`
            };
        }
    } catch (err: any) {
        if (err.code === 'NotFound') {
            return {
                exists: false,
                message: `File ${fileName} not found in bucket ${bucket}`
            };
        }
        // For any other error, return error message
        return {
            exists: false,
            message: `Error checking file existence: ${err.message}`
        };
    }
}
