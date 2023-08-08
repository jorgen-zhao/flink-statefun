package org.apache.flink.statefun.sdk.nats.ingress;

/** Position for the ingress to start consuming AWS Kinesis shards. */
public abstract class NatsIngressStartupPosition {

  private NatsIngressStartupPosition() {}

  /** Start consuming from the earliest position possible. */
  public static NatsIngressStartupPosition fromEarliest() {
    return EarliestPosition.INSTANCE;
  }

  /** Start consuming from the latest position, i.e. head of the stream shards. */
  public static NatsIngressStartupPosition fromLatest() {
    return LatestPosition.INSTANCE;
  }

  /** Checks whether this position is configured using the earliest position. */
  public final boolean isEarliest() {
    return getClass() == EarliestPosition.class;
  }

  /** Checks whether this position is configured using the latest position. */
  public final boolean isLatest() {
    return getClass() == LatestPosition.class;
  }


  @SuppressWarnings("WeakerAccess")
  public static final class EarliestPosition extends NatsIngressStartupPosition {
    private static final EarliestPosition INSTANCE = new EarliestPosition();
  }

  @SuppressWarnings("WeakerAccess")
  public static final class LatestPosition extends NatsIngressStartupPosition {
    private static final LatestPosition INSTANCE = new LatestPosition();
  }
}
