package net.devaction.kafka.transfersrecordingservice.core;

import java.math.BigDecimal;

import net.devaction.entity.AccountBalanceEntity;
import net.devaction.entity.TransferEntity;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class NewAccountBalanceProvider{
    
    public AccountBalanceEntity provide(AccountBalanceEntity currentAB, 
            TransferEntity transferEntity) {
        
        BalanceAndVersion currentBalanceAndVersion = new BalanceAndVersion(
                currentAB.getBalance(), currentAB.getVersion());
        
        return provide(currentAB.getAccountId(), currentAB.getClientId(), 
                currentBalanceAndVersion, transferEntity.getId(), 
                transferEntity.getAmount());
    }
    public AccountBalanceEntity provide(String accountId, String clientId, 
            BalanceAndVersion currentBalanceAndVersion, String transferId,  
            BigDecimal transferAmount){
        
        AccountBalanceEntity entity = new AccountBalanceEntity();
        entity.setAccountId(accountId);
        entity.setClientId(clientId);
        
        BalanceAndVersion newBalanceAndVersion = provideNew(currentBalanceAndVersion, transferAmount);
        entity.setBalance(newBalanceAndVersion.getBalance());
        entity.setVersion(newBalanceAndVersion.getVersion());
        entity.setTransferId(transferId);
        
        return entity;
    }
    
    BalanceAndVersion provideNew(BalanceAndVersion current, BigDecimal transferamount) {
        // Note: so far we are assuming that negative balances are OK
        BigDecimal newBalance = current.getBalance().add(transferamount);
        long newVersion = current.getVersion() + 1;
        
        return new BalanceAndVersion(newBalance, newVersion);
    }
}
