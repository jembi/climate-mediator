import { EachMessagePayload } from 'kafkajs';

export interface KafkaConsumer {
  /**
   * Get the list of topics that this consumer is interested in.
   * This method should return an array of topic names that the consumer will subscribe to.
   */
  getTopicsInterestedIn: () => Promise<string[]>;

  /**
   * Consume a message from the Kafka topic.
   * This method is called when a message is received from the topic.
   */
  onConsumeMessage: (messagePayload: EachMessagePayload) => Promise<void>;
}
