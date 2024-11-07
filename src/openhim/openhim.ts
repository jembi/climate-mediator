import logger from "../logger";
import { MediatorConfig } from "../types/mediatorConfig";
import { RequestOptions } from "../types/request";
import { getConfig } from "../config/config";
import {
  activateHeartbeat,
  fetchConfig,
  registerMediator,
} from "openhim-mediator-utils";

const resolveMediatorConfig = (
  mediatorConfigFilePath: string,
): MediatorConfig => {
  let mediatorConfigFile;

  try {
    mediatorConfigFile = require(mediatorConfigFilePath);
  } catch (error) {
    logger.error(`Failed to parse JSON: ${error}`);
    throw error;
  }

  return mediatorConfigFile;
};

const resolveOpenhimConfig = (urn: string): RequestOptions => {
  const config = getConfig();

  return {
    username: config.openhimUsername,
    password: config.openhimPassword,
    apiURL: config.openhimMediatorUrl,
    trustSelfSigned: config.trustSelfSigned,
    urn: urn,
  };
};

export const setupMediator = (mediatorConfigFilePath: string) => {
  try {
    const mediatorConfig = resolveMediatorConfig(mediatorConfigFilePath);
    const openhimConfig = resolveOpenhimConfig(mediatorConfig.urn);

    registerMediator(openhimConfig, mediatorConfig, (error: Error) => {
      if (error) {
        logger.error(`Failed to register mediator: ${JSON.stringify(error)}`);
        throw error;
      }

      logger.info("Successfully registered mediator!");

      fetchConfig(openhimConfig, (err: Error) => {
        if (err) {
          logger.error(
            `Failed to fetch initial config: ${JSON.stringify(err)}`,
          );
          throw err;
        }

        const emitter = activateHeartbeat(openhimConfig);

        emitter.on("error", (err: Error) => {
          logger.error(`Heartbeat failed: ${JSON.stringify(err)}`);
        });
      });
    });
  } catch (err) {
    logger.error("Unable to register mediator", err);
  }
};
