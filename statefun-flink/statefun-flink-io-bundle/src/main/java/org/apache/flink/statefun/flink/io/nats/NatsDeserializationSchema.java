package org.apache.flink.statefun.flink.io.nats;

import io.nats.client.Message;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

@PublicEvolving
public interface NatsDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {
    default void open(DeserializationSchema.InitializationContext context) throws Exception {
    }

    boolean isEndOfStream(T var1);

    T deserialize(Message var1) throws Exception;

    default void deserialize(Message message, Collector<T> out) throws Exception {
        T deserialized = this.deserialize(message);
        if (deserialized != null) {
            out.collect(deserialized);
        }

    }
}
