package net.devaction.kafka.transfersrecordingservice.main;

// We are aware that this class is not part of the Java API
// but we need it
import sun.misc.Signal;
import sun.misc.SignalHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.kafka.transfersrecordingservice.joinstreams.TransfersRecordingServiceJoinStreams;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TransfersRecordingServiceMain implements SignalHandler {
    private static final Logger log = LoggerFactory.getLogger(TransfersRecordingServiceMain.class);

    private static final String WINCH_SIGNAL = "WINCH";

    private TransfersRecordingService service;

    public static void main(String[] args) {
        Implementation implementation = Implementation.POLLING;

        if (args.length > 0 && args[0].equalsIgnoreCase(Implementation.JOIN.name())) {
            implementation = Implementation.JOIN;
        }

        new TransfersRecordingServiceMain().run(implementation);
    }

    private void run(Implementation implementation) {
        registerThisAsOsSignalHandler();

        if (implementation == Implementation.POLLING)
            service = new TransfersRecordingServicePolling();
        else
            service = new TransfersRecordingServiceJoinStreams();


        TransfersRecordingServiceRunnable runnable =
                new TransfersRecordingServiceRunnable(service);

        Thread pollingThread = new Thread(runnable);
        pollingThread.setName("polling-thread");

        pollingThread.start();

        try {
            pollingThread.join();
        } catch (InterruptedException ex) {
            log.error("Interrupted while waiting for the \"polling\" thread to finish", ex);
            Thread.currentThread().interrupt();
        }

        log.info("Exiting main thread.");
    }

    @Override
    public void handle(Signal signal) {
        log.info("We have received the signal to tell us to stop: {}", signal.getName());
        service.stop();
    }

    private void registerThisAsOsSignalHandler() {
        log.debug("Going to register this object to handle the {} signal", WINCH_SIGNAL);
        try {
            Signal.handle(new Signal(WINCH_SIGNAL), this);
        } catch(Exception ex) {
            // Most likely this is a signal that's not supported on this
            // platform or with the JVM as it is currently configured
            log.error("FATAL: The signal is not supported: {}, exiting", WINCH_SIGNAL, ex);
            System.exit(1);
        }
    }
}

enum Implementation {POLLING, JOIN}
