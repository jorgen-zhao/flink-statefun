package org.apache.flink.statefun.flink.io.nats;

import com.google.auto.service.AutoService;
import org.apache.flink.statefun.flink.io.kafka.KafkaSinkProvider;
import org.apache.flink.statefun.flink.io.kafka.KafkaSourceProvider;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.apache.flink.statefun.sdk.kafka.Constants;

import java.util.Map;

@AutoService(FlinkIoModule.class)
public final class NatsFlinkModule implements FlinkIoModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        binder.bindSourceProvider(Constants.KAFKA_INGRESS_TYPE, new KafkaSourceProvider());
        binder.bindSinkProvider(Constants.KAFKA_EGRESS_TYPE, new KafkaSinkProvider());
    }
}
