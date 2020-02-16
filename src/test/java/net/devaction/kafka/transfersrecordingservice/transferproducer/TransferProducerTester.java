package net.devaction.kafka.transfersrecordingservice.transferproducer;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.entity.TransferEntity;
import net.devaction.kafka.transfersrecordingservice.config.ConfigReader;
import net.devaction.kafka.transfersrecordingservice.config.ConfigValues;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TransferProducerTester {
    private static final Logger log = LoggerFactory.getLogger(TransferProducerTester.class);

    public static void main(String[] args) {
        new TransferProducerTester().run();
    }

    private void run() {
        ConfigValues configValues;
        try {
            configValues = new ConfigReader().read();
        } catch (Exception ex) {
            log.error("Unable to read the configuration values, exiting");
            return;
        }

        TransferProducer transferProducer = new TransferProducer();
        transferProducer.start(configValues.getBootstrapServers(),
                configValues.getSchemaRegistryUrl());

        // TransferEntity transferEntity = new TransferEntity("28a090daa002",
        //        new BigDecimal("-100.54"));

        // We let everybody know that a new account needs to be created,
        // by publishing a transaction with amount = zero.
        // TransferEntity transferEntity = new TransferEntity("account-05", BigDecimal.ZERO);
        // TransferEntity transferEntity = new TransferEntity("account-04", BigDecimal.ZERO);
        // TransferEntity transferEntity = new TransferEntity("account-04", BigDecimal.valueOf(15, 1)); // 1.5
        // TransferEntity transferEntity = new TransferEntity("account-04", BigDecimal.valueOf(2));
        TransferEntity transferEntity = new TransferEntity("account-05", BigDecimal.valueOf(11));
        // TransferEntity transferEntity = new TransferEntity("account-01", BigDecimal.valueOf(5));

        transferProducer.send(transferEntity);

        log.info("Sleeping while the message is sent");
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ex) {
            log.error(ex.toString(), ex);
        }

        transferProducer.stop();
    }
}
