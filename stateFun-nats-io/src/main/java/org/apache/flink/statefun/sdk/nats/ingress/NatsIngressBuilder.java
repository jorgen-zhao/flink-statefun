package org.apache.flink.statefun.sdk.nats.ingress;

import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.core.OptionalProperty;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * A builder for creating an {@link IngressSpec} for consuming data from Nats.
 *
 * @param <T> The type consumed from Nats.
 */
public final class NatsIngressBuilder<T> {

    private final IngressIdentifier<T> id;
    private OptionalProperty<String> credentialPath = OptionalProperty.withoutDefault();
    private OptionalProperty<String> url = OptionalProperty.withoutDefault();

    private final List<String> streams = new ArrayList<>();
    private NatsIngressDeserializer<T> deserializer;
    private NatsIngressStartupPosition startupPosition = NatsIngressStartupPosition.fromLatest();

    /**
     * Contains properties for both the underlying AWS client, as well as Flink-connector specific
     * properties.
     */
    private final Properties properties = new Properties();

    private NatsIngressBuilder(IngressIdentifier<T> id) {
        this.id = Objects.requireNonNull(id);
    }

    /**
     * @param id  A unique ingress identifier.
     * @param <T> The type consumed from Nats.
     * @return A new {@link NatsIngressBuilder}.
     */
    public static <T> NatsIngressBuilder<T> forIdentifier(IngressIdentifier<T> id) {
        return new NatsIngressBuilder<>(id);
    }

    /**
     * @param stream The name of a stream that should be consumed.
     */
    public NatsIngressBuilder<T> withStream(String stream) {
        this.streams.add(stream);
        return this;
    }

    /**
     * @param url The url of a nats that should be connected.
     */
    public NatsIngressBuilder<T> withUrl(String url) {
        this.url.set(url);
        return this;
    }

    /**
     * @param credentialPath The credential of a nats that should be use.
     */
    public NatsIngressBuilder<T> withCredentialPath(String credentialPath) {
        this.credentialPath.set(credentialPath);
        return this;
    }

    /**
     * @param streams A list of streams that should be consumed.
     */
    public NatsIngressBuilder<T> withStreams(List<String> streams) {
        this.streams.addAll(streams);
        return this;
    }

    /**
     * @param deserializerClass The deserializer used to convert between Nats's byte messages and
     *                          Java objects.
     */
    public NatsIngressBuilder<T> withDeserializer(Class<? extends NatsIngressDeserializer<T>> deserializerClass) {
        Objects.requireNonNull(deserializerClass);
        this.deserializer = instantiateDeserializer(deserializerClass);
        return this;
    }

    /**
     * Configures the position that the ingress should start consuming from. By default, the startup
     * position is {@link NatsIngressStartupPosition#fromLatest()}.
     *
     * <p>Note that this configuration only affects the position when starting the application from a
     * fresh start. When restoring the application from a savepoint, the ingress will always start
     * consuming from the position persisted in the savepoint.
     *
     * @param startupPosition the position that the Kafka ingress should start consuming from.
     * @see NatsIngressStartupPosition
     */
    public NatsIngressBuilder<T> withStartupPosition(
            NatsIngressStartupPosition startupPosition) {
        this.startupPosition = Objects.requireNonNull(startupPosition);
        return this;
    }


    /**
     * Sets a AWS client configuration to be used by the ingress.
     *
     * <p>Supported values are properties of AWS's <a
     * href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html">com.aws.ClientConfiguration</a>.
     * For example, to set a value for {@code SOCKET_TIMEOUT}, the property key would be {@code
     * SocketTimeout}.
     *
     * @param key   the property to set.
     * @param value the value for the property.
     * @see <a
     * href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html">com.aws.ClientConfiguration</a>.
     * @deprecated Please use {@link #withProperty(String, String)} instead.
     */
    @Deprecated
    public NatsIngressBuilder<T> withClientConfigurationProperty(String key, String value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        this.properties.setProperty(key, value);
        return this;
    }

    public NatsIngressBuilder<T> withProperty(String key, String value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        this.properties.setProperty(key, value);
        return this;
    }

    public NatsIngressBuilder<T> withProperties(Properties properties) {
        Objects.requireNonNull(properties);
        this.properties.putAll(properties);
        return this;
    }

    /**
     * @return A new {@link NatsIngressSpec}.
     */
    public NatsIngressSpec<T> build() {
        return new NatsIngressSpec<>(
                id, streams, deserializer, startupPosition, properties);
    }

    // ========================================================================================
    //  Methods for runtime usage
    // ========================================================================================

    @ForRuntime
    public NatsIngressBuilder<T> withDeserializer(NatsIngressDeserializer<T> deserializer) {
        this.deserializer = Objects.requireNonNull(deserializer);
        return this;
    }
    // ========================================================================================
    //  Utility methods
    // ========================================================================================

    private static <T extends NatsIngressDeserializer<?>> T instantiateDeserializer(Class<T> deserializerClass) {
        try {
            Constructor<T> defaultConstructor = deserializerClass.getDeclaredConstructor();
            defaultConstructor.setAccessible(true);
            return defaultConstructor.newInstance();
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                    "Unable to create an instance of deserializer "
                            + deserializerClass.getName()
                            + "; has no default constructor",
                    e);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException(
                    "Unable to create an instance of deserializer " + deserializerClass.getName(), e);
        }
    }
}
