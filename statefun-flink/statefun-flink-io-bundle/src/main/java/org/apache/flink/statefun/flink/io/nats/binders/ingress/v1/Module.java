package org.apache.flink.statefun.flink.io.nats.binders.ingress.v1;//package org.apache.flink.statefun.flink.io.nats.binders.ingress.v1;

import com.google.auto.service.AutoService;
import org.apache.flink.statefun.extensions.ExtensionModule;

import java.util.Map;

@AutoService(ExtensionModule.class)
public final class Module implements ExtensionModule {

    @Override
    public void configure(Map<String, String> globalConfigurations, Binder universeBinder) {
        universeBinder.bindExtension(
                RoutableNatsIngressBinderV1.KIND_TYPE, RoutableNatsIngressBinderV1.INSTANCE);
    }
}
