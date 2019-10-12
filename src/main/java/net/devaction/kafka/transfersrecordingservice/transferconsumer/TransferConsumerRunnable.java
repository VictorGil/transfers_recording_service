package net.devaction.kafka.transfersrecordingservice.transferconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TransferConsumerRunnable implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(TransferConsumerRunnable.class);

    private final TransferConsumer consumer;

    public TransferConsumerRunnable(TransferConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run(){
        log.info("Going to start the \"Transfer\" consumer: {}", consumer);
        consumer.start();
    }
}


