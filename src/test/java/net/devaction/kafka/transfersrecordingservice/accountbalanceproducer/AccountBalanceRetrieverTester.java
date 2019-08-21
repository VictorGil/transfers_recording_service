package net.devaction.kafka.transfersrecordingservice.accountbalanceproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class AccountBalanceRetrieverTester{
    private static final Logger log = LoggerFactory.getLogger(AccountBalanceRetrieverTester.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081"; 
    
    private AccountBalanceRetriever retriever;
    
    public static void main(String[] args){
        new AccountBalanceRetrieverTester().run(args);
    }

    private void run(String[] accountIds) {
        retriever = new AccountBalanceRetrieverImpl();
        
        retriever.start(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL);
        
        for (int i = 0; i < accountIds.length; i++)
            printAccountBalance(accountIds[i]);
        
        retriever.stop();
    }
    
    /**
     * Please note that this will not work unless the 
     * local data store has been created by the same JVM process
     */
    void printAccountBalance(String accountId) {
        log.info("Current balance for account {}: {}", accountId, 
                retriever.retrieve(accountId));
    }    
}
