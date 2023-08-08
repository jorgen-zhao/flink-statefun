package org.apache.flink.statefun.flink.io.nats;

import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaTopicPartition;
import org.apache.flink.statefun.sdk.nats.ingress.NatsIngressSpec;
import org.apache.flink.statefun.sdk.nats.ingress.NatsIngressStartupPosition;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;

public class NatsSourceProvider implements SourceProvider {

    @Override
    public <T> SourceFunction forSpec(IngressSpec<T> ingressSpec) {
        NatsIngressSpec<T> spec = asNatsSpec(ingressSpec);

        return new NatsConsumer(spec.topics(), deserializationSchemaFromSpec(spec), spec.properties());
//        configureStartupPosition(consumer, spec.startupPosition());
    }

    private static <T> NatsIngressSpec<T> asNatsSpec(IngressSpec<T> ingressSpec) {
        if (ingressSpec instanceof NatsIngressSpec) {
            return (NatsIngressSpec<T>) ingressSpec;
        }
        if (ingressSpec == null) {
            throw new NullPointerException("Unable to translate a NULL spec");
        }
        throw new IllegalArgumentException(String.format("Wrong type %s", ingressSpec.type()));
    }

//    private static <T> void configureStartupPosition(
//            NatsConsumer<T> consumer, NatsIngressStartupPosition startupPosition) {
//        if (startupPosition.isEarliest()) {
//            consumer.setStartFromEarliest();
//        } else if (startupPosition.isLatest()) {
//            consumer.setStartFromLatest();
//        } else {
//            throw new IllegalStateException("Safe guard; should not occur");
//        }
//    }

    private <T> NatsDeserializationSchema<T> deserializationSchemaFromSpec(
            NatsIngressSpec<T> spec) {
    return new NatsDeserializationSchemaDelegate<>(spec.deserializer());
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
