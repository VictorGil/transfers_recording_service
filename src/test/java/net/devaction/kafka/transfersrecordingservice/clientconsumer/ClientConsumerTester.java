package net.devaction.kafka.transfersrecordingservice.clientconsumer;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class ClientConsumerTester {
    private static final Logger log = LoggerFactory.getLogger(ClientConsumerTester.class);

    public static void main(String[] args) {
        new ClientConsumerTester().run();
    }

    private void run() {
        log.info("Starting.");

        ClientProcessor processor = new SimpleClientProcessorImpl();
        ClientConsumer consumer = new ClientConsumer(
                "localhost:9092", "http://localhost:8081", processor);

        consumer.setSeekFromBeginningOn();

        ClientConsumerRunnable runnable = new ClientConsumerRunnable(consumer);

        Thread thread = new Thread(runnable);
        thread.setName("client-consumer-thread");

        thread.start();

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException ex) {
            log.error(" {}", ex, ex);
        }

        consumer.stop();
        try {
            thread.join();
        } catch (InterruptedException ex) {
            log.error(" {}", ex, ex);
        }

        log.info("Exiting");
    }
}
