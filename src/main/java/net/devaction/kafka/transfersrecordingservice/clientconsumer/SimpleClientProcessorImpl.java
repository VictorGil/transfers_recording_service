package net.devaction.kafka.transfersrecordingservice.clientconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.kafka.avro.Client;
import net.devaction.kafka.avro.util.ClientConverter;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class SimpleClientProcessorImpl implements ClientProcessor{
    private static final Logger log = LoggerFactory.getLogger(SimpleClientProcessorImpl.class);

    @Override
    public void process(Client client){
        log.info("Client data processed: {}",
                ClientConverter.convertToPojo(client));
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
