import express from "express";
import * as Minio from "minio";
import path from "path";
import { getConfig } from "./config/config";
import logger from "./logger";
import routes from "./routes/index";
import { setupMediator } from "./openhim/openhim";
import { validateJsonFile, validateCsvFile } from "./utils/file-validators";
import { readFile, rm } from "fs/promises";

const config = getConfig();
const app = express();

app.use("/", routes);

if (config.runningMode !== "testing") {
  app.listen(config.port, () => {
    logger.info(`Server is running on port - ${config.port}`);

    if (config.registerMediator) {
      setupMediator(path.resolve(__dirname, "./openhim/mediatorConfig.json"));
    }
  });
}

const { bucket, endPoint, port, useSSL, accessKey, secretKey, prefix, suffix } = config.minio

const minioClient = new Minio.Client({
  endPoint,
  port,
  useSSL,
  accessKey,
  secretKey
});

const listener = minioClient.listenBucketNotification(bucket, prefix, suffix, ['s3:ObjectCreated:*'])

listener.on('notification', async (notification) => {
  //@ts-ignore
  const file = notification.s3.object.key

  //@ts-ignore
  minioClient.fGetObject(bucket, file, `tmp/${file}`,async(err) => {
    if (err) {
      console.error(err)
    } else {
      console.log("File downloaded");
      const fileBuffer = await readFile(`tmp/${file}`);

      //get the file extension
      const extension = file.split(".").pop();
      console.log(extension);
      

      if (extension === "json" && validateJsonFile(fileBuffer)) {
        console.log("JSON file");
      }
      else if (extension === "csv" && validateCsvFile(fileBuffer)) {
        console.log("CSV file");
      } else {
        console.log("Unknown file type");
      }
      await rm(`tmp/${file}`);
    }
  })
})