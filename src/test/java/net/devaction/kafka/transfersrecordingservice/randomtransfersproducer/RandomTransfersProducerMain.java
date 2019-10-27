package net.devaction.kafka.transfersrecordingservice.randomtransfersproducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since October 2019
 */
public class RandomTransfersProducerMain {
    private static final Logger log = LoggerFactory.getLogger(RandomTransfersProducerMain.class);

    private final RandomTransferProducer producer = new RandomTransferProducer();

    public static void main(String[] args) {
        new RandomTransfersProducerMain().run();
    }

    private void run() {
        startProducerOnNewThread();
        waitForKeyStroke();
        producer.stop();
        log.info("Main method finished");
    }

    private void startProducerOnNewThread() {
        Thread thread = new Thread(producer);
        thread.setName(producer.getClass().getSimpleName() + "-thread");
        thread.start();
    }

    private void waitForKeyStroke() {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("Please press a key to stop the test WebSockets client.");
        try {
            reader.readLine();
        } catch (IOException ex) {
            log.error("{}", ex, ex);
        }
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException ex) {
            log.error("{}", ex, ex);
        }
        log.info("Exiting");
    }
}
