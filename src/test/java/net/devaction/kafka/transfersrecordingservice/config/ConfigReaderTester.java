package net.devaction.kafka.transfersrecordingservice.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class ConfigReaderTester{
    private static final Logger log = LoggerFactory.getLogger(ConfigReaderTester.class);

    public static void main(String[] args){
        new ConfigReaderTester().run();
    }

    private void run(){
        ConfigValues configValues;
        try{
            configValues = new ConfigReader().read();
        } catch (Exception ex){
            log.error("Error when trying to read the config values, exiting");
            return;
        }

        log.info("Config values read from file: {}", configValues);
    }
}

