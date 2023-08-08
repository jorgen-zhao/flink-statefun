package org.apache.flink.statefun.flink.io.nats.binders.ingress.v1;

import com.google.protobuf.Message;
import com.google.protobuf.MoreByteStrings;
import io.nats.client.impl.NatsJetStreamMetaData;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.sdk.nats.ingress.NatsIngressDeserializer;

import java.util.Map;

public final class RoutableNatsIngressDeserializer implements NatsIngressDeserializer<Message> {

  private static final long serialVersionUID = 1L;

  private final Map<String, RoutingConfig> routingConfigs;

  public RoutableNatsIngressDeserializer(Map<String, RoutingConfig> routingConfigs) {
    if (routingConfigs == null || routingConfigs.isEmpty()) {
      throw new IllegalArgumentException(
          "Routing config for routable Kafka ingress cannot be empty.");
    }
    this.routingConfigs = routingConfigs;
  }

  @Override
  public Message deserialize(io.nats.client.Message message) {
    NatsJetStreamMetaData metaData = message.metaData();
    final String topic =  metaData.getStream();
    final byte[] payload = message.getData();
    final String key = topic.substring(4);

    final RoutingConfig routingConfig = routingConfigs.get(topic);
    if (routingConfig == null) {
      throw new IllegalStateException(
          "Consumed a record from topic [" + topic + "], but no routing config was specified.");
    }
    return AutoRoutable.newBuilder()
        .setConfig(routingConfig)
        .setId(key)
        .setPayloadBytes(MoreByteStrings.wrap(payload))
        .build();
  }
}
