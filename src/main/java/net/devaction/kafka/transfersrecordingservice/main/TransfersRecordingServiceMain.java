package net.devaction.kafka.transfersrecordingservice.main;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TransfersRecordingServiceMain{
    private static final Logger log = LoggerFactory.getLogger(TransfersRecordingServiceMain.class);
    
    public static void main(String[] args) {
        new TransfersRecordingServiceMain().run();
    }
    
    private void run() {        
        TransfersRecordingService service = new TransfersRecordingService();
        
        TransfersRecordingServiceRunnable runnable = 
                new TransfersRecordingServiceRunnable(service);
        
        Thread thread = new Thread(runnable);
        thread.setName("polling-thread");
        
        thread.start();
        
        log.info("Sleeping for 5 minutes");
        try{
            TimeUnit.MINUTES.sleep(5);
        } catch (InterruptedException ex){
            log.error("{}", ex, ex);
            Thread.currentThread().interrupt();
        }
        
        service.stop();        
    }
}

