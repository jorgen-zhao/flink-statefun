package org.apache.flink.statefun.sdk.nats.ingress;

import io.nats.client.Message;

import java.io.Serializable;

/**
 * Describes how to deserialize {@link Message}s consumed from AWS Kinesis into data types
 * that are processed by the system.
 *
 * @param <T> The type created by the ingress deserializer.
 */
public interface NatsIngressDeserializer<T> extends Serializable {

  /**
   * Deserialize an input value from a {@link Message} consumed from AWS Kinesis.
   *
   * @param message the {@link Message} consumed from AWS Kinesis.
   * @return the deserialized data object.
   */
  T deserialize(Message message);
}
