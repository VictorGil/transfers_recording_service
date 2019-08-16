package net.devaction.kafka.transfersrecordingservice.accountbalanceproducer;

import net.devaction.entity.AccountBalanceEntity;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public interface AccountBalanceProducer{

    public void start(String bootstrapServers, String schemaRegistryUrl);
    
    public void send(AccountBalanceEntity accountBalanceEntity);
    
    public void stop();
}


