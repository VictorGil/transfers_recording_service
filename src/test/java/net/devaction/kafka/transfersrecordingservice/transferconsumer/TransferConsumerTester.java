package net.devaction.kafka.transfersrecordingservice.transferconsumer;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TransferConsumerTester{
    private static final Logger log = LoggerFactory.getLogger(TransferConsumerTester.class);

    public static void main(String[] args){
        new TransferConsumerTester().run();
    }

    private void run(){
        log.info("Starting.");

        TransferProcessor processor = new SimpleTransferProcessorImpl();
        TransferConsumer consumer = new TransferConsumer(
                "localhost:9092", "http://localhost:8081", processor);

        consumer.setSeekFromBeginningOn();

        TransferConsumerRunnable runnable = new TransferConsumerRunnable(consumer);

        Thread thread = new Thread(runnable);
        thread.setName("transfer-consumer-thread");

        thread.start();

        try{
            TimeUnit.SECONDS.sleep(60);
        } catch (InterruptedException ex){
            log.error("{}", ex, ex);
        }

        consumer.stop();
        try{
            thread.join();
        } catch (InterruptedException ex){
            log.error("{}", ex, ex);
        }

        log.info("Exiting");
    }
}


