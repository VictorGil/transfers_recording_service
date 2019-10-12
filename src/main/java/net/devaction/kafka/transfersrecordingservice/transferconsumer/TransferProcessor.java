package net.devaction.kafka.transfersrecordingservice.transferconsumer;

import net.devaction.kafka.avro.Transfer;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public interface TransferProcessor {

    public void process(Transfer transfer);
}
