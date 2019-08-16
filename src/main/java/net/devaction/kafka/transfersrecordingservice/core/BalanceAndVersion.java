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

    @Override
    public int hashCode(){
        final int prime = 31;
        int result = 1;
        
        result = prime * result + ((balance == null) ? 0 : balance.hashCode());
        result = prime * result + (int) (version ^ (version >>> 32));
        
        return result;
    }

    @Override
    public boolean equals(Object obj){
        if (this == obj)
            return true;
        
        if (obj == null)
            return false;
        
        if (getClass() != obj.getClass())
            return false;
        
        BalanceAndVersion other = (BalanceAndVersion) obj;
        
        if (balance == null){
            if (other.balance != null)
                return false;
            // Note that we should not use "equals" method
            // to find out whether two BigDecimals represent
            // the same numeric value
        } else if (balance.compareTo(other.balance) != 0)
            return false;
        
        return version == other.version;
    }

    public BigDecimal getBalance(){
        return balance;
    }

    public long getVersion(){
        return version;
    }    
}
