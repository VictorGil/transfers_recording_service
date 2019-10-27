package net.devaction.kafka.transfersrecordingservice.producers;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.entity.AccountBalanceEntity;
import net.devaction.entity.TransferEntity;
import net.devaction.kafka.transfersrecordingservice.accountbalanceproducer.AccountBalanceProducer;
import net.devaction.kafka.transfersrecordingservice.accountbalanceproducer.AccountBalanceProducerImpl;
import net.devaction.kafka.transfersrecordingservice.config.ConfigReader;
import net.devaction.kafka.transfersrecordingservice.config.ConfigValues;
import net.devaction.kafka.transfersrecordingservice.transferproducer.TransferProducer;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class ProducersTester {
    private static final Logger log = LoggerFactory.getLogger(ProducersTester.class);

    ConfigValues configValues;

    AccountBalanceProducer abProducer;
    TransferProducer transferProducer;

    public static void main(String[] args) {
        Action action = Action.SEND_TRANSFERS;

        if (args.length > 0 && args[0].equalsIgnoreCase(Action.INITIALIZE.name())) {
            action = Action.INITIALIZE;
        }

        new ProducersTester().run(action);
    }

    private void run(Action action) {
        if (action == Action.INITIALIZE) {
            initializeAccounts();
        }

        if (action == Action.SEND_TRANSFERS) {
            sendTransfers();
        }
    }

    private void initializeAccounts() {
        readConfigValues();

        startABproducer();

        // sendAccountBalances();
        // sendAccountBalances2();
        sendAccountBalances3();

        stopABproducer();
    }

    private void sendTransfers() {
        readConfigValues();

        startTransferProducer();

        sendTransfers1();
        // sendTransfers2();

        stopTransferProducer();
    }

    private void readConfigValues() {
        try {
            configValues = new ConfigReader().read();
        } catch (Exception ex) {
            log.error("Unable to read the configuration values, exiting");
            System.exit(1);
        }
    }

    private void startABproducer() {
        abProducer = new AccountBalanceProducerImpl();
        abProducer.start(configValues.getBootstrapServers(),
                configValues.getSchemaRegistryUrl());
    }

    private void sendAccountBalances() {
        AccountBalanceEntity abEntity1 = new AccountBalanceEntity(
                "28a090daa001", "334490daa001");
        abProducer.send(abEntity1);

        AccountBalanceEntity abEntity2 = new AccountBalanceEntity(
                "28a090daa002", "334490daa002");
        abProducer.send(abEntity2);

        sleep(500);
    }

    private void sendAccountBalances2() {
        AccountBalanceEntity abEntity1 = new AccountBalanceEntity(
                "28a090daa005", "334490daa005");
        abProducer.send(abEntity1);

        AccountBalanceEntity abEntity2 = new AccountBalanceEntity(
                "28a090daa006", "334490daa006");
        abProducer.send(abEntity2);

        sleep(500);
    }

    private void sendAccountBalances3() {
        for (int i = 0; i < 10; i++) {
            AccountBalanceEntity abEntity1 = new AccountBalanceEntity(
                    "acc-0" + i, generateRandomId());
            abProducer.send(abEntity1);
        }

        AccountBalanceEntity abEntity1 = new AccountBalanceEntity(
                "acc-10", generateRandomId());
        abProducer.send(abEntity1);

        sleep(500);
    }

    private void stopABproducer() {
        abProducer.stop();
    }

    private void startTransferProducer() {
        transferProducer = new TransferProducer();
        transferProducer.start(configValues.getBootstrapServers(),
                configValues.getSchemaRegistryUrl());
    }

    private void sendTransfers1() {
        TransferEntity transferEntity1 = new TransferEntity("28a090daa001",
                new BigDecimal("30.75"));
        transferProducer.send(transferEntity1);

        TransferEntity transferEntity2 = new TransferEntity("28a090daa002",
                new BigDecimal("20"));
        transferProducer.send(transferEntity2);

        sleep(300);

        TransferEntity transferEntity3 = new TransferEntity("28a090daa001",
                new BigDecimal("5"));
        transferProducer.send(transferEntity3);

        TransferEntity transferEntity4 = new TransferEntity("28a090daa002",
                new BigDecimal("-51.83"));
        transferProducer.send(transferEntity4);

        sleep(200);

        TransferEntity transferEntity5 = new TransferEntity("28a090daa001",
                new BigDecimal("-7.83"));
        transferProducer.send(transferEntity5);

        TransferEntity transferEntity6 = new TransferEntity("28a090daa002",
                new BigDecimal("100"));
        transferProducer.send(transferEntity6);

        sleep(200);

        TransferEntity transferEntity7 = new TransferEntity("28a090daa001",
                new BigDecimal("11.00005"));
        transferProducer.send(transferEntity7);

        sleep(100);
    }

    private void sendTransfers2() {
        TransferEntity transferEntity1 = new TransferEntity("28a090daa005",
                new BigDecimal("30.75"));
        transferProducer.send(transferEntity1);

        TransferEntity transferEntity2 = new TransferEntity("28a090daa006",
                new BigDecimal("20"));
        transferProducer.send(transferEntity2);

        sleep(1000);

        TransferEntity transferEntity3 = new TransferEntity("28a090daa005",
                new BigDecimal("5"));
        transferProducer.send(transferEntity3);

        TransferEntity transferEntity4 = new TransferEntity("28a090daa006",
                new BigDecimal("-51.83"));
        transferProducer.send(transferEntity4);

        sleep(1000);

        TransferEntity transferEntity5 = new TransferEntity("28a090daa005",
                new BigDecimal("-7.83"));
        transferProducer.send(transferEntity5);

        TransferEntity transferEntity6 = new TransferEntity("28a090daa006",
                new BigDecimal("100"));
        transferProducer.send(transferEntity6);

        sleep(1000);

        TransferEntity transferEntity7 = new TransferEntity("28a090daa005",
                new BigDecimal("11.00005"));
        transferProducer.send(transferEntity7);

        sleep(500);
    }

    private void stopTransferProducer() {
        transferProducer.stop();
    }

    private void sleep(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException ex) {
            log.error(ex.toString(), ex);
        }
    }

    private String generateRandomId() {
        // last 12 hexadecimal digits of the random UUID
        return UUID.randomUUID().toString().substring(24);
    }
}

enum Action { INITIALIZE, SEND_TRANSFERS }
