package net.devaction.kafka.transfersrecordingservice.randomtransfersproducer;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.entity.TransferEntity;
import net.devaction.kafka.transfersrecordingservice.config.ConfigReader;
import net.devaction.kafka.transfersrecordingservice.config.ConfigValues;
import net.devaction.kafka.transfersrecordingservice.transferproducer.TransferProducer;

/**
 * @author Víctor Gil
 *
 * since October 2019
 */
public class RandomTransferProducer implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(RandomTransferProducer.class);

    private final Random random = new Random();

    private volatile boolean stop;

    private ConfigValues configValues;
    private final TransferProducer producer = new TransferProducer();

    private final RandomTransferGenerator transferGenerator = new RandomTransferGenerator();

    @Override
    public void run() {
        start();
    }

    private void start() {
        readConfigValues();

        producer.start(configValues.getBootstrapServers(),
                configValues.getSchemaRegistryUrl());

        while (!stop) {
            sleep();
            TransferEntity transfer = transferGenerator.generateTransfer();
            producer.send(transfer);
        }

        producer.stop();
        log.info("Execution of this class finished.");
    }

    public void stop() {
        log.info("We have been told to stop.");
        stop = true;
    }

    private void sleep() {
        long millis = getRandomMillis();

        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException ex) {
            log.error("Interrupted while sleeping", ex);
        }
    }

    private long getRandomMillis() {
        int low = 10;
        int high = 500;
        // number between 10 (inclusive) and 500 (inclusive)
        return random.nextInt(high + 1 - low) + low;
    }

    private void readConfigValues() {
        try {
            configValues = new ConfigReader().read();
        } catch (Exception ex) {
            log.error("Unable to read the configuration values, exiting");
            System.exit(1);
        }
    }
}
