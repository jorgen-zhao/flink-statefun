package org.apache.flink.statefun.flink.io.nats.binders.ingress.v1;

import com.google.protobuf.Message;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.io.common.json.IngressIdentifierJsonDeserializer;
import org.apache.flink.statefun.flink.io.common.json.PropertiesJsonDeserializer;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1.RoutableKafkaIngressDeserializer;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.kafka.*;
import org.apache.flink.statefun.sdk.nats.NatsIngressBuilderApiExtension;
import org.apache.flink.statefun.sdk.nats.ingress.NatsIngressBuilder;
import org.apache.flink.statefun.sdk.nats.ingress.NatsIngressSpec;
import org.apache.flink.statefun.sdk.nats.ingress.NatsIngressStartupPosition;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@JsonDeserialize(builder = RoutableNatsIngressSpec.Builder.class)
final class RoutableNatsIngressSpec {

    private final IngressIdentifier<Message> id;
    private final Optional<String> url;
    private final Optional<String> credentialPath;
    private final Map<String, RoutingConfig> topicRoutings;
    private final NatsIngressStartupPosition startupPosition;
    private final Properties properties;

    private RoutableNatsIngressSpec(
            IngressIdentifier<Message> id,
            Optional<String> url,
            Optional<String> credentialPath,
            Map<String, RoutingConfig> topicRoutings,
            NatsIngressStartupPosition startupPosition,
            Properties properties) {
        this.id = id;
        this.url = url;
        this.credentialPath = credentialPath;
        this.topicRoutings = topicRoutings;
        this.startupPosition = startupPosition;
        this.properties = properties;
    }

    public IngressIdentifier<Message> id() {
        return id;
    }

    public NatsIngressSpec toUniversalNatsIngressSpec() {
        final NatsIngressBuilder<Message> builder = NatsIngressBuilder.forIdentifier(id);
        url.ifPresent(builder::withUrl);
        credentialPath.ifPresent(builder::withCredentialPath);
        topicRoutings.keySet().forEach(builder::withStream);
        builder.withStartupPosition(startupPosition);
        builder.withProperties(properties);
        NatsIngressBuilderApiExtension.withDeserializer(
                builder, new RoutableNatsIngressDeserializer(topicRoutings));

        return builder.build();
    }

    @JsonPOJOBuilder
    public static class Builder {

        private final IngressIdentifier<Message> id;

        private Optional<String> url = Optional.empty();
        private Optional<String> credentialPath = Optional.empty();
        private Map<String, RoutingConfig> topicRoutings = new HashMap<>();
        private NatsIngressStartupPosition startupPosition = NatsIngressStartupPosition.fromLatest();
        private Properties properties = new Properties();

        @JsonCreator
        private Builder(
                @JsonProperty("id") @JsonDeserialize(using = IngressIdentifierJsonDeserializer.class)
                IngressIdentifier<Message> id) {
            this.id = Objects.requireNonNull(id);
        }

        @JsonProperty("url")
        public Builder withUrl(String url) {
            Objects.requireNonNull(url);
            this.url = Optional.of(url);
            return this;
        }

        @JsonProperty("credentialPath")
        public Builder withCredentialPath(String credentialPath) {
            Objects.requireNonNull(credentialPath);
            this.credentialPath = Optional.of(credentialPath);
            return this;
        }

        @JsonProperty("streams")
        @JsonDeserialize(using = TopicRoutingsJsonDeserializer.class)
        public Builder withStreamRoutings(Map<String, RoutingConfig> topicRoutings) {
            this.topicRoutings = Objects.requireNonNull(topicRoutings);
            return this;
        }

        @JsonProperty("startupPosition")
        @JsonDeserialize(using = StartupPositionJsonDeserializer.class)
        public Builder withStartupPosition(NatsIngressStartupPosition startupPosition) {
            this.startupPosition = Objects.requireNonNull(startupPosition);
            return this;
        }

        @JsonProperty("properties")
        @JsonDeserialize(using = PropertiesJsonDeserializer.class)
        public Builder withProperties(Properties properties) {
            this.properties = Objects.requireNonNull(properties);
            return this;
        }

        public RoutableNatsIngressSpec build() {
            return new RoutableNatsIngressSpec(
                    id,
                    url,
                    credentialPath,
                    topicRoutings,
                    startupPosition,
                    properties);
        }
    }

    private static class TopicRoutingsJsonDeserializer
            extends JsonDeserializer<Map<String, RoutingConfig>> {
        @Override
        public Map<String, RoutingConfig> deserialize(
                JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            final ObjectNode[] routingJsonNodes = jsonParser.readValueAs(ObjectNode[].class);

            final Map<String, RoutingConfig> result = new HashMap<>(routingJsonNodes.length);
            for (ObjectNode routingJsonNode : routingJsonNodes) {
                final RoutingConfig routingConfig =
                        RoutingConfig.newBuilder()
                                .setTypeUrl(routingJsonNode.get("valueType").textValue())
                                .addAllTargetFunctionTypes(parseTargetFunctions(routingJsonNode))
                                .build();
                result.put(routingJsonNode.get("stream").asText(), routingConfig);
            }
            return result;
        }
    }

    private static class StartupPositionJsonDeserializer extends JsonDeserializer<NatsIngressStartupPosition> {
        private static final String STARTUP_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS Z";
        private static final DateTimeFormatter STARTUP_DATE_FORMATTER =
                DateTimeFormatter.ofPattern(STARTUP_DATE_PATTERN);

        @Override
        public NatsIngressStartupPosition deserialize(
                JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            final ObjectNode startupPositionNode = jsonParser.readValueAs(ObjectNode.class);
            final String startupTypeString = startupPositionNode.get("type").asText();
            switch (startupTypeString) {
                case "earliest":
                    return NatsIngressStartupPosition.fromEarliest();
                case "latest":
                    return NatsIngressStartupPosition.fromLatest();
                // TODO 待完善
//                case "specific-offsets":
//                    return NatsIngressStartupPosition.fromSpecificOffsets(
//                            parseSpecificStartupOffsetsMap(startupPositionNode));
//                case "date":
//                    return NatsIngressStartupPosition.fromDate(parseStartupDate(startupPositionNode));
                default:
                    throw new IllegalArgumentException(
                            "Invalid startup position type: "
                                    + startupTypeString
                                    + "; valid values are [group-offsets, earliest, latest, specific-offsets, date]");
            }
        }
    }

    private static List<TargetFunctionType> parseTargetFunctions(JsonNode routingJsonNode) {
        final Iterable<JsonNode> targetFunctionNodes = routingJsonNode.get("targets");
        return StreamSupport.stream(targetFunctionNodes.spliterator(), false)
                .map(RoutableNatsIngressSpec::parseTargetFunctionType)
                .collect(Collectors.toList());
    }

    private static TargetFunctionType parseTargetFunctionType(JsonNode targetFunctionNode) {
        final TypeName targetType = TypeName.parseFrom(targetFunctionNode.asText());
        return TargetFunctionType.newBuilder()
                .setNamespace(targetType.namespace())
                .setType(targetType.name())
                .build();
    }
}
