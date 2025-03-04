import { createLogger, format, transports } from 'winston';
import { getConfig } from './config/config';

const logger = createLogger({
  level: getConfig().logLevel,
  format: format.combine(
    format.colorize(),
    format.timestamp(),
    format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level}]: ${message}`;
    })
  ),
  transports: [
    new transports.Console(),
    // You can add more transports like File if needed
  ],
});

export default logger;
