import * as Minio from "minio";
import { getConfig } from '../config/config';

const {endPoint, port, useSSL, bucketRegion, accessKey, secretKey, prefix, suffix } =
    getConfig().minio;

/**
 * Uploads a file to Minio storage
 * @param {string} sourceFile - Path to the file to upload
 * @param {string} destinationObject - Name for the uploaded object
 * @param {Object} [customMetadata={}] - Optional custom metadata
 * @returns {Promise<void>}
 */
export async function uploadToMinio(sourceFile: string, destinationObject: string, bucket: string, customMetadata = {}) {
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
        console.log(`Bucket ${bucket} created in "${bucketRegion}".`);
    }

    // Set the object metadata
    const metaData = {
        'Content-Type': 'text/plain',
        ...customMetadata
    };

    // Upload the file
    await minioClient.fPutObject(bucket, destinationObject, sourceFile, metaData);
    console.log(`File ${sourceFile} uploaded as object ${destinationObject} in bucket ${bucket}`);
}
