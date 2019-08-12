package net.devaction.kafka.transfersrecordingservice.clientproducer;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.entity.ClientEntity;
import net.devaction.kafka.transfersrecordingservice.clientproducer.ClientProducer;
import net.devaction.kafka.transfersrecordingservice.config.ConfigReader;
import net.devaction.kafka.transfersrecordingservice.config.ConfigValues;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class ClientProducerTester{
    private static final Logger log = LoggerFactory.getLogger(ClientProducerTester.class);
    
    public static void main(final String[] args) {
        new ClientProducerTester().run();
    }
    
    private void run() {
        ClientProducer clientProducer = new ClientProducer();
        
        ConfigValues configValues;
        try{
            configValues = new ConfigReader().read();
        } catch (Exception ex){
            log.error("Unable to read the configuration values, exiting");
            return;
        }
        
        clientProducer.start(configValues.getBootstrapServers(), 
                configValues.getSchemaRegistryUrl());
        
        ClientEntity clientEntity = new ClientEntity();
        clientEntity.generateId();
        clientEntity.setFirstName("Jessica");
        clientEntity.setLastName("Andrew");
        clientEntity.setEmail("j.andrew@gmx.com");
        clientEntity.setAddress("Purple Street 29");
        clientEntity.setLevel("bronze");
        
        clientProducer.send(clientEntity);
        
        log.info("Sleeping while the message is sent");
        try{
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException ex){
            log.error(ex.toString(), ex);
        }
        
        clientProducer.stop();        
    }
}
