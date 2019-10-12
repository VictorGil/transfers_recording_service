package net.devaction.kafka.transfersrecordingservice.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.kafka.transfersrecordingservice.accountbalanceproducer.AccountBalanceProducer;
import net.devaction.kafka.transfersrecordingservice.accountbalanceproducer.AccountBalanceProducerImpl;
import net.devaction.kafka.transfersrecordingservice.accountbalanceretriever.AccountBalanceRetriever;
import net.devaction.kafka.transfersrecordingservice.accountbalanceretriever.AccountBalanceRetrieverImpl;
import net.devaction.kafka.transfersrecordingservice.config.ConfigReader;
import net.devaction.kafka.transfersrecordingservice.config.ConfigValues;
import net.devaction.kafka.transfersrecordingservice.core.NewAccountBalanceProvider;
import net.devaction.kafka.transfersrecordingservice.transferconsumer.TransferConsumer;
import net.devaction.kafka.transfersrecordingservice.transferconsumer.TransferProcessorImpl;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TransfersRecordingServicePolling implements TransfersRecordingService {
    private static final Logger log = LoggerFactory.getLogger(TransfersRecordingServicePolling.class);

    private AccountBalanceProducer abProducer;
    private AccountBalanceRetriever abRetriever;
    private TransferConsumer transferConsumer;

    private Thread pollingThread;

    @Override
    public void start() {
        ConfigValues configValues;
        log.info("Going to read the configuration values");
        try{
            configValues = new ConfigReader().read();
        } catch (Exception ex){
            log.error("Unable to read the configuration values, exiting");
            return;
        }

        final String bootstrapServers = configValues.getBootstrapServers();
        final String schemaRegistryUrl = configValues.getSchemaRegistryUrl();

        abProducer = new AccountBalanceProducerImpl();
        abRetriever = new AccountBalanceRetrieverImpl();
        NewAccountBalanceProvider newABprovider = new NewAccountBalanceProvider();

        TransferProcessorImpl transferProcessor = new TransferProcessorImpl();
        transferProcessor.setAccountBalanceProducer(abProducer);
        transferProcessor.setAccountBalanceRetriever(abRetriever);
        transferProcessor.setNewABprovider(newABprovider);

        transferConsumer = new TransferConsumer(bootstrapServers,
                schemaRegistryUrl, transferProcessor);

        log.info("Going to start the {}", AccountBalanceRetriever.class.getSimpleName());
        abRetriever.start(bootstrapServers, schemaRegistryUrl);

        log.info("Going to start the {}", AccountBalanceProducer.class.getSimpleName());
        abProducer.start(bootstrapServers, schemaRegistryUrl);

        pollingThread = Thread.currentThread();

        log.info("Going to start the {}", TransferConsumer.class.getSimpleName());
        transferConsumer.start();
    }

    @Override
    public void stop() {
        log.info("We have been told to stop.");

        if (transferConsumer != null) {
            log.info("Going to stop the \"Transfer\" consumer");
            transferConsumer.stop();
        }

        if (abProducer != null) {
            log.info("Going to stop the \"AccountBalance\" producer");
            abProducer.stop();
        }

        if (abRetriever != null) {
            log.info("Going to stop the \"AccountBalance\" retriever "
                    + "(and the local queryable data store)");
            abRetriever.stop();
        }

        if (pollingThread != null) {
            try{
                pollingThread.join();
            } catch (InterruptedException ex){
                log.error("Interrupted while waiting for the \"polling\" thread to finish", ex);
                Thread.currentThread().interrupt();
            }
        }
    }
}
