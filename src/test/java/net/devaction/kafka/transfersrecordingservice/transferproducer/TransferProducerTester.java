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
public class TransferProducerTester{
    private static final Logger log = LoggerFactory.getLogger(TransferProducerTester.class);

    public static void main(String[] args){
        new TransferProducerTester().run();
    }
    
    private void run() {
        TransferProducer transferProducer = new TransferProducer();
        
        ConfigValues configValues;
        try{
            configValues = new ConfigReader().read();
        } catch (Exception ex){
            log.error("Unable to read the configuration values, exiting");
            return;
        }
        
        transferProducer.start(configValues.getBootstrapServers(), 
                configValues.getSchemaRegistryUrl());
        
        TransferEntity transferEntity = new TransferEntity("28a090d01b07", 
                new BigDecimal("30.75"));

        transferProducer.send(transferEntity);
        
        log.info("Sleeping while the message is sent");
        try{
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ex){
            log.error(ex.toString(), ex);
        }
        
        transferProducer.stop();        
    }
}
