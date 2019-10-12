package net.devaction.kafka.transfersrecordingservice.clientconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class ClientConsumerRunnable implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(ClientConsumerRunnable.class);

    private final ClientConsumer consumer;

    public ClientConsumerRunnable(ClientConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        log.info("Going to start the \"Client\" consumer: {}", consumer);
        consumer.start();
    }
}
