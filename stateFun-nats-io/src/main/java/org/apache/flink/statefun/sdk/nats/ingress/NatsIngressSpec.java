package org.apache.flink.statefun.sdk.nats.ingress;

import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.nats.NatsIOTypes;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

public final class NatsIngressSpec<T> implements IngressSpec<T> {
  private final IngressIdentifier<T> ingressIdentifier;
  private final List<String> streams;
  private final NatsIngressDeserializer<T> deserializer;
  private final NatsIngressStartupPosition startupPosition;
  private final Properties properties;

  NatsIngressSpec(
          IngressIdentifier<T> ingressIdentifier,
          List<String> streams,
          NatsIngressDeserializer<T> deserializer,
          NatsIngressStartupPosition startupPosition,
          Properties properties) {
    this.ingressIdentifier = Objects.requireNonNull(ingressIdentifier, "ingress identifier");
    this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
    this.startupPosition = Objects.requireNonNull(startupPosition, "startup position");

    this.properties = Objects.requireNonNull(properties);

    this.streams = Objects.requireNonNull(streams, "Nats stream names");
    if (streams.isEmpty()) {
      throw new IllegalArgumentException(
              "Must have at least one stream to consume from specified.");
    }
  }

  @Override
  public IngressIdentifier<T> id() {
    return ingressIdentifier;
  }

  @Override
  public IngressType type() {
    return NatsIOTypes.UNIVERSAL_INGRESS_TYPE;
  }

  public List<String> streams() {
    return streams;
  }

  public NatsIngressDeserializer<T> deserializer() {
    return deserializer;
  }

  public NatsIngressStartupPosition startupPosition() {
    return startupPosition;
  }

  public Properties properties() {
    return properties;
  }
}
