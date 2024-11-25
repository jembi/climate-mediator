import logger from '../logger';
import { MediatorConfig } from '../types/mediatorConfig';
import { RequestOptions } from '../types/request';
import { getConfig } from '../config/config';
import { activateHeartbeat, fetchConfig, registerMediator } from 'openhim-mediator-utils';

const { openhimUsername, openhimPassword, openhimMediatorUrl, trustSelfSigned } = getConfig();

const resolveMediatorConfig = (mediatorConfigFilePath: string): MediatorConfig => {
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
  return {
    username: openhimUsername,
    password: openhimPassword,
    apiURL: openhimMediatorUrl,
    trustSelfSigned: trustSelfSigned,
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

      logger.info('Successfully registered mediator!');

      fetchConfig(openhimConfig, (err: Error) => {
        if (err) {
          logger.error(`Failed to fetch initial config: ${JSON.stringify(err)}`);
          throw err;
        }

        const emitter = activateHeartbeat(openhimConfig);

        emitter.on('error', (err: Error) => {
          logger.error(`Heartbeat failed: ${JSON.stringify(err)}`);
        });

        emitter.on('config', (config: any) => {
          logger.info(`Config: ${JSON.stringify(config)}`);
        });
      });
    });
  } catch (err) {
    logger.error('Unable to register mediator', err);
  }
};
