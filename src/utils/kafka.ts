import { Consumer, ConsumerSubscribeTopics, EachMessagePayload, Kafka } from 'kafkajs';
import logger from '../logger';
import { processMessageByTopic } from '../services/kafka/messageHandlers';

/**
 * Sets up Kafka consumers based on the topics specified in the KAFKA_TOPICS environment variable.
 * It connects to the Kafka broker, subscribes to the specified topics, and processes incoming messages.
 *
 * @throws Will throw an error if KAFKA_TOPICS, KAFKA_CLIENT_ID, KAFKA_GROUP_ID, or KAFKA_BROKERS environment variables are not set.
 */
export async function setupKafkaConsumers() {
  try {
    const kafkaConsumer = await createKafkaConsumer();
    const eachMessage = async (messagePayload: EachMessagePayload) => {
      const { topic, partition, message } = messagePayload;

      logger.info(
        `Received message from topic ${topic} partition ${partition}: ${message.value?.toString('utf8')}`
      );

      try {
        await processMessageByTopic(messagePayload);
      } catch (err) {
        logger.error(`Error processing message from topic ${topic}: ${err}`);
      }
    };

    await kafkaConsumer.run({ eachMessage });
  } catch (err) {
    logger.error(`Error setting up Kafka consumers: ${err}`);
    throw err;
  }
}

async function createKafkaConsumer(): Promise<Consumer> {
  const kafkaClientId = process.env.KAFKA_CLIENT_ID;
  if (!kafkaClientId) {
    logger.error('KAFKA_CLIENT_ID environment variable is not set');
    throw new Error('KAFKA_CLIENT_ID environment variable is not set');
  }

  const kafkaGroupId = process.env.KAFKA_GROUP_ID;
  if (!kafkaGroupId) {
    logger.error('KAFKA_GROUP_ID environment variable is not set');
    throw new Error('KAFKA_GROUP_ID environment variable is not set');
  }

  const kafkaBrokers = process.env.KAFKA_BROKERS;
  if (!kafkaBrokers) {
    logger.error('KAFKA_BROKERS environment variable is not set');
    throw new Error('KAFKA_BROKERS environment variable is not set');
  }
  const kafkaBrokersArray = kafkaBrokers.split(',').map((broker) => broker.trim());
  if (kafkaBrokersArray.length === 0) {
    logger.error('No brokers found in KAFKA_BROKERS environment variable');
    throw new Error('No brokers found in KAFKA_BROKERS environment variable');
  }

  const topicsFromEvn = process.env.KAFKA_TOPICS;
  if (!topicsFromEvn) {
    logger.error('KAFKA_TOPICS environment variable is not set');
    throw new Error('KAFKA_TOPICS environment variable is not set');
  }
  const topics = topicsFromEvn.split(',').map((topic) => topic.trim());
  if (topics.length === 0) {
    logger.error('No topics found in KAFKA_TOPICS environment variable');
    throw new Error('No topics found in KAFKA_TOPICS environment variable');
  }

  const topicSubscription: ConsumerSubscribeTopics = {
    topics,
    fromBeginning: false,
  };

  const kafka = new Kafka({
    clientId: kafkaClientId,
    brokers: kafkaBrokersArray,
  });
  const consumer = kafka.consumer({ groupId: kafkaGroupId });

  await consumer.connect();
  await consumer.subscribe(topicSubscription);

  setupGracefulShutdown(consumer);

  logger.info(`Kafka consumer connected and subscribed to topics: ${topics.toString()}`);

  return consumer;
}

function setupGracefulShutdown(consumer: Consumer) {
  const errorTypes = ['unhandledRejection', 'uncaughtException'];

  errorTypes.forEach((type) => {
    process.on(type, async (e) => {
      try {
        console.log(`process.on ${type}`);
        console.error(e);
        await consumer.disconnect();
        process.exit(0);
      } catch (err) {
        process.exit(1);
      }
    });
  });
}
