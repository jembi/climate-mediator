import { EachMessagePayload } from 'kafkajs';

export interface KafkaConsumer {
  /**
   * Get the Kafka topic ID for the consumer.
   * The topic ID is used to subscribe to the topic and consume messages.
   */
  getTopicID: () => Promise<string>;

  /**
   * Consume a message from the Kafka topic.
   * This method is called when a message is received from the topic.
   */
  onConsumeMessage: (messagePayload: string) => Promise<void>;
}
