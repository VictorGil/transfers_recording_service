package net.devaction.kafka.transfersrecordingservice;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TestProducerCallBack implements Callback{
    private static final Logger log = LoggerFactory.getLogger(TestProducerCallBack.class);

    @Override
    public void onCompletion(final RecordMetadata metadata, final Exception exception){
        if (exception != null) {
            log.error(exception.toString(), exception);
            log.error("Record metadata: {}", metadata);
        } else{
            log.debug("Record metadata: {}", metadata);
        }
    }
}
