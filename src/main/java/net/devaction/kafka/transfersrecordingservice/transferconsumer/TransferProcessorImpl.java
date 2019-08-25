package net.devaction.kafka.transfersrecordingservice.transferconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.entity.AccountBalanceEntity;
import net.devaction.entity.TransferEntity;
import net.devaction.kafka.avro.Transfer;
import net.devaction.kafka.avro.util.TransferConverter;
import net.devaction.kafka.transfersrecordingservice.accountbalanceproducer.AccountBalanceProducer;
import net.devaction.kafka.transfersrecordingservice.accountbalanceretriever.AccountBalanceRetriever;
import net.devaction.kafka.transfersrecordingservice.core.NewAccountBalanceProvider;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TransferProcessorImpl implements TransferProcessor{
    private static final Logger log = LoggerFactory.getLogger(TransferProcessorImpl.class);

    private AccountBalanceRetriever accountBalanceRetriever; 
    private NewAccountBalanceProvider newABprovider;
    private AccountBalanceProducer accountBalanceProducer;
    
    @Override
    public void process(Transfer transfer){
        TransferEntity entity = TransferConverter.convertToPojo(transfer);
        log.debug("Transfer data to be processed: {}", entity);
       
        process(entity);
    }
    
    void process(TransferEntity transferEntity){
        AccountBalanceEntity currentAB = accountBalanceRetriever.retrieve(
                transferEntity.getAccountId());
        
        if (currentAB == null)
            return;
        
        AccountBalanceEntity newAB = newABprovider.provide(currentAB, transferEntity);
        
        accountBalanceProducer.send(newAB);
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    public void setAccountBalanceRetriever(AccountBalanceRetriever accountBalanceRetriever){
        this.accountBalanceRetriever = accountBalanceRetriever;
    }

    public void setNewABprovider(NewAccountBalanceProvider newABprovider){
        this.newABprovider = newABprovider;
    }

    public void setAccountBalanceProducer(AccountBalanceProducer accountBalanceProducer){
        this.accountBalanceProducer = accountBalanceProducer;
    }
}
