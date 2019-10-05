package net.devaction.kafka.transfersrecordingservice.accountbalanceproducer;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.entity.AccountBalanceEntity;
import net.devaction.kafka.transfersrecordingservice.config.ConfigReader;
import net.devaction.kafka.transfersrecordingservice.config.ConfigValues;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class AccountBalanceProducerTester{
    private static final Logger log = LoggerFactory.getLogger(
            AccountBalanceProducerTester.class);
    
    public static void main(String[] args){
        new AccountBalanceProducerTester().run();
    }
    
    private void run() {        
        ConfigValues configValues;
        try{
            configValues = new ConfigReader().read();
        } catch (Exception ex){
            log.error("Unable to read the configuration values, exiting");
            return;
        }
        
        AccountBalanceProducer producer = new AccountBalanceProducerImpl();
        producer.start(configValues.getBootstrapServers(), 
                configValues.getSchemaRegistryUrl());
        
        AccountBalanceEntity abEntity = createABentity();
        
        producer.send(abEntity);
        log.info("Sleeping while the message is sent");
        try{
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ex){
            log.error(ex.toString(), ex);
        }
        
        producer.stop();        
    }
    
    private AccountBalanceEntity createABentity() {
        AccountBalanceEntity abEntity = new AccountBalanceEntity(
                "28a090daa002", "334490daa002", "test-transfer-04", new BigDecimal("140"), 4L);
        return abEntity;
    }
    
    private AccountBalanceEntity createInitialABentity() {
        AccountBalanceEntity abEntity = new AccountBalanceEntity(
                "28a090daa001", "334490daa001");
        return abEntity;
    }
}
