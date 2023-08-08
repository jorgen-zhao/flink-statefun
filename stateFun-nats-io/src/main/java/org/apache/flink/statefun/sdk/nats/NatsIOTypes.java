package org.apache.flink.statefun.sdk.nats;

import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.IngressType;

public class NatsIOTypes {
    private NatsIOTypes() {}

    public static final IngressType UNIVERSAL_INGRESS_TYPE =
            new IngressType("statefun.nats.io", "universal-ingress");
    public static final EgressType UNIVERSAL_EGRESS_TYPE =
            new EgressType("statefun.nats.io", "universal-egress");
}
