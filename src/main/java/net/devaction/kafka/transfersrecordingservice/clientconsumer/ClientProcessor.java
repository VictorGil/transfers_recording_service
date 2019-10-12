package net.devaction.kafka.transfersrecordingservice.clientconsumer;

import net.devaction.kafka.avro.Client;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public interface ClientProcessor{

    public void process(Client client);
}
