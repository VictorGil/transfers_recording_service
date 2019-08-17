package net.devaction.kafka.transfersrecordingservice.joinstreams;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.kstream.Consumed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import net.devaction.kafka.avro.AccountBalance;
import net.devaction.kafka.avro.Transfer;
import net.devaction.kafka.streams.ExceptionHandler;
import net.devaction.kafka.transfersrecordingservice.config.ConfigReader;
import net.devaction.kafka.transfersrecordingservice.config.ConfigValues;
import net.devaction.kafka.transfersrecordingservice.main.TransfersRecordingService;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TransfersRecordingServiceJoinStreams 
        implements TransfersRecordingService{
    
    private static final Logger log = LoggerFactory.getLogger(
            TransfersRecordingServiceJoinStreams.class);

    private KafkaStreams streams;
    
    private static final String ACCOUNT_BALANCES_TOPIC = "account-balances";
    
    private static final String TRANSFERS_TOPIC = "transfers";
    
    @Override
    public void start() {
        ConfigValues configValues;
        log.info("Going to read the configuration values");
        try{
            configValues = new ConfigReader().read();
        } catch (Exception ex){
            log.error("Unable to read the configuration values, exiting");
            return;
        }
        
        final Properties streamsConfigProperties = new Properties();
        streamsConfigProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, 
                "account-balance-streams-retriever");
        
        streamsConfigProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
                configValues.getBootstrapServers());
        
        streamsConfigProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        KeyValueBytesStoreSupplier clientsStoreSupplier = 
                Stores.inMemoryKeyValueStore("account-balance-store");
        
        final Serde<String> stringSerde = Serdes.String();
        final Serde<AccountBalance> accountBalanceSerde = new SpecificAvroSerde<>();
        
        final boolean isKeySerde = false;
        accountBalanceSerde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
                        configValues.getSchemaRegistryUrl()),
                isKeySerde);

        final StreamsBuilder builder = new StreamsBuilder(); 
        
        final KTable<String,AccountBalance> accountBalancesKTable = builder.table(
                ACCOUNT_BALANCES_TOPIC,
                Materialized.<String,AccountBalance>as(clientsStoreSupplier)
                        .withKeySerde(stringSerde)
                        .withValueSerde(accountBalanceSerde)
                        .withCachingDisabled());
        
        final Serde<Transfer> transferSerde = new SpecificAvroSerde<>();
        transferSerde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
                        configValues.getSchemaRegistryUrl()),
                isKeySerde);
        
        final KStream<String, Transfer> transfersKStream = 
                builder.stream(TRANSFERS_TOPIC,  
                Consumed.with(stringSerde, transferSerde));
        
        final KStream<String, AccountBalance> newABstream = transfersKStream.leftJoin(
                accountBalancesKTable, new TransferAndAccountBalanceJoiner());
        
        newABstream.to(ACCOUNT_BALANCES_TOPIC, Produced.with(stringSerde, accountBalanceSerde));
        
        streams = new KafkaStreams(builder.build(), streamsConfigProperties);
        streams.setUncaughtExceptionHandler(new ExceptionHandler());
        
        streams.start();       

        while (streams.state() != State.RUNNING) {
            try{
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException ex){
                log.error("Interrupted while waiting for the \"Streams\" to start.");
                Thread.currentThread().interrupt();
            }
        }
        
        log.info("\"Streams\" started");
    }

    @Override
    public void stop(){
        log.info("Going to close the \"Streams\"");
        streams.close();        
    }
}
