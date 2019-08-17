package net.devaction.kafka.transfersrecordingservice.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TransfersRecordingServiceRunnable implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(TransfersRecordingServiceRunnable.class);

    private final TransfersRecordingService service;
    
    
    public TransfersRecordingServiceRunnable(TransfersRecordingService service){
        this.service = service;
    }

    @Override
    public void run(){
        log.info("Going to start the {}", 
                service.getClass().getSimpleName());
        
        service.start();
    }
}
