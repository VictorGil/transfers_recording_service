package net.devaction.kafka.transfersrecordingservice.joinstreams;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.entity.AccountBalanceEntity;
import net.devaction.entity.TransferEntity;
import net.devaction.kafka.avro.AccountBalance;
import net.devaction.kafka.avro.Transfer;
import net.devaction.kafka.avro.util.AccountBalanceConverter;
import net.devaction.kafka.avro.util.TransferConverter;
import net.devaction.kafka.transfersrecordingservice.core.NewAccountBalanceProvider;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TransferAndAccountBalanceJoiner implements 
        ValueJoiner<Transfer, AccountBalance, AccountBalance>{
    
    private static final Logger log = LoggerFactory.getLogger(TransferAndAccountBalanceJoiner.class);

    private final NewAccountBalanceProvider newABprovider;
    
    public TransferAndAccountBalanceJoiner(){
        newABprovider = new NewAccountBalanceProvider();
    }  
    
    @Override
    public AccountBalance apply(Transfer transfer, AccountBalance currentAB){
        
        TransferEntity tEntity = TransferConverter.convertToPojo(transfer);
        log.debug("Transfer received for the join: {}", tEntity);
        
        AccountBalanceEntity abEntity = AccountBalanceConverter.convertToPojo(currentAB);
        log.debug("\"AccountBalance\" retrieved from the local store for the join: {}", abEntity);
        
        AccountBalanceEntity newABentity = newABprovider.provide(abEntity, tEntity);
        log.debug("New \"AccountBalance\": {}", newABentity);
        
        return AccountBalanceConverter.convertToAvro(newABentity);
    }

}


