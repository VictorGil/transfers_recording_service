package net.devaction.kafka.transfersrecordingservice.accountbalanceretriever;

import net.devaction.entity.AccountBalanceEntity;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public interface AccountBalanceRetriever{

    public void start(String bootstrapServers, String schemaRegistryUrl);

    public AccountBalanceEntity retrieve(String accountId);

    public void stop();
}
