package net.devaction.kafka.streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class ExceptionHandler implements Thread.UncaughtExceptionHandler{
    private static final Logger log = LoggerFactory.getLogger(ExceptionHandler.class);

    @Override
    public void uncaughtException(Thread thread, Throwable ex){
        log.error("Uncaught exception thrown by {}: {}", thread, ex, ex);        
    }
}
