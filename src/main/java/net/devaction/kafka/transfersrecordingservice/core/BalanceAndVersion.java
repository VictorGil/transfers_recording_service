package net.devaction.kafka.transfersrecordingservice.core;

import java.math.BigDecimal;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class BalanceAndVersion{

    private final BigDecimal balance;
    private final long version;
    
    public BalanceAndVersion(BigDecimal balance, long version){
        this.balance = balance;
        this.version = version;
    }

    @Override
    public String toString(){
        return "BalanceAndVersion [balance=" + balance + ", version=" + version + "]";
    }

    public BigDecimal getBalance(){
        return balance;
    }

    public long getVersion(){
        return version;
    }    
}
