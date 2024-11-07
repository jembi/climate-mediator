import express from "express";
import path from "path";
import { getConfig } from "./config/config";
import logger from "./logger";
import routes from "./routes/index";
import { setupMediator } from "./openhim/openhim";

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
