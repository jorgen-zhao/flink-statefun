package org.apache.flink.statefun.flink.io.nats;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.flink.common.UnimplementedTypeInfo;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Objects;

final class NatsDeserializationSchemaDelegate<T> implements KafkaDeserializationSchema<T> {

  private static final long serialVersionUID = 1;

  private final TypeInformation<T> producedTypeInfo;
  private final KafkaIngressDeserializer<T> delegate;

  NatsDeserializationSchemaDelegate(KafkaIngressDeserializer<T> delegate) {
    this.producedTypeInfo = new UnimplementedTypeInfo<>();
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public boolean isEndOfStream(T t) {
    return false;
  }

  @Override
  public T deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
    return delegate.deserialize(consumerRecord);
  }

  @Override
  public TypeInformation<T> getProducedType() {
    // this would never be actually used, it would be replaced during translation with the type
    // information
    // of IngressIdentifier's producedType.
    // see: Sources#setOutputType.
    // if this invriant would not hold in the future, this type information would produce a
    // serialier
    // that fails immediately.
    return producedTypeInfo;
  }
}
