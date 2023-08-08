package org.apache.flink.statefun.flink.io.nats;

import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressStartupPosition;
import org.apache.flink.statefun.sdk.kafka.KafkaTopicPartition;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.HashMap;
import java.util.Map;

public class NatsSourceProvider implements SourceProvider {

  @Override
  public <T> SourceFunction<T> forSpec(IngressSpec<T> ingressSpec) {
    KafkaIngressSpec<T> spec = asKafkaSpec(ingressSpec);

    FlinkKafkaConsumer<T> consumer =
        new FlinkKafkaConsumer<>(
            spec.topics(), deserializationSchemaFromSpec(spec), spec.properties());
    configureStartupPosition(consumer, spec.startupPosition());
    return consumer;
  }

  private static <T> KafkaIngressSpec<T> asKafkaSpec(IngressSpec<T> ingressSpec) {
    if (ingressSpec instanceof KafkaIngressSpec) {
      return (KafkaIngressSpec<T>) ingressSpec;
    }
    if (ingressSpec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }
    throw new IllegalArgumentException(String.format("Wrong type %s", ingressSpec.type()));
  }

  private static <T> void configureStartupPosition(
      FlinkKafkaConsumer<T> consumer, KafkaIngressStartupPosition startupPosition) {
    if (startupPosition.isGroupOffsets()) {
      consumer.setStartFromGroupOffsets();
    } else if (startupPosition.isEarliest()) {
      consumer.setStartFromEarliest();
    } else if (startupPosition.isLatest()) {
      consumer.setStartFromLatest();
    } else if (startupPosition.isSpecificOffsets()) {
      KafkaIngressStartupPosition.SpecificOffsetsPosition offsetsPosition =
          startupPosition.asSpecificOffsets();
      consumer.setStartFromSpecificOffsets(
          convertKafkaTopicPartitionMap(offsetsPosition.specificOffsets()));
    } else if (startupPosition.isDate()) {
      KafkaIngressStartupPosition.DatePosition datePosition = startupPosition.asDate();
      consumer.setStartFromTimestamp(datePosition.epochMilli());
    } else {
      throw new IllegalStateException("Safe guard; should not occur");
    }
  }

  private <T> KafkaDeserializationSchema<T> deserializationSchemaFromSpec(
      KafkaIngressSpec<T> spec) {
//    return new KafkaDeserializationSchemaDelegate<>(spec.deserializer());
    // FIXME
    return null;
  }

  private static Map<
          org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition, Long>
      convertKafkaTopicPartitionMap(Map<KafkaTopicPartition, Long> offsets) {
    Map<org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition, Long> result =
        new HashMap<>(offsets.size());
    for (Map.Entry<KafkaTopicPartition, Long> offset : offsets.entrySet()) {
      result.put(convertKafkaTopicPartition(offset.getKey()), offset.getValue());
    }

    return result;
  }

  private static org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
      convertKafkaTopicPartition(KafkaTopicPartition partition) {
    return new org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition(
        partition.topic(), partition.partition());
  }
}
