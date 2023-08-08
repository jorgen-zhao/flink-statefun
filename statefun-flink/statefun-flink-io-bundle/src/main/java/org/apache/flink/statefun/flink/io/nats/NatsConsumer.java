package org.apache.flink.statefun.flink.io.nats;

import io.nats.client.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class NatsConsumer implements SourceFunction<Message> {

    final LinkedBlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private volatile boolean isRunning;

    private final String url;

    private final String credentials;

    //    private final List<String> streamNames;
    private final String streamName;

    private final String consumerName;

    public NatsConsumer(String url, String credentials, String streamName, String consumerName) {
        this.url = url;
        this.credentials = credentials;
        this.streamName = streamName;
        this.consumerName = consumerName;
    }

    @Override
    public void run(SourceContext<Message> ctx) throws Exception {

        isRunning = true;

        if (isRunning) {

            try (Connection natsConn = Nats.connect(url, Nats.credentials(credentials))) {
//                streamNames.forEach(streamName -> {
                try {
                    natsConn.jetStream().subscribe(null, natsConn.createDispatcher(), (message) -> {
//                                byte[] data = message.getData();
//                                NatsJetStreamMetaData metaData = message.metaData();
//                                String streamName1 = metaData.getStream();
//                                String subject = message.getSubject();
//                                String robotId = streamName1.substring(4);
//                                long consumerSequence = metaData.consumerSequence();

//                                    final boolean enqueued = inbox.get(streamName1).offer(new MessageProxy(streamName1, subject, robotId, consumerSequence, data));
                                final boolean enqueued = inbox.offer(message);
                                message.ack();
                            },
                            false,
                            PushSubscribeOptions.bind(streamName, consumerName));
                } catch (IOException | JetStreamApiException e) {
                    throw new RuntimeException(e);
                }
//                });

                while (isRunning) {
                    final Message msg = inbox.poll();
                    if (msg != null) {
                        ctx.collect(msg);
                    }
                }
                natsConn.close();
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
