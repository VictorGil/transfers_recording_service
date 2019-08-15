package net.devaction.kafka.transfersrecordingservice.streams;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.kafka.avro.Client;
import net.devaction.kafka.streams.ExceptionHandler;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class ReadValueExampleMain{
    private static final Logger log = LoggerFactory.getLogger(ReadValueExampleMain.class);
    
    public static void main(String[] args){
        new ReadValueExampleMain().run();
    }
    
    private void run(){
        final Properties streamsConfigProperties = new Properties();
        streamsConfigProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "clients-store-subservice");
        streamsConfigProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfigProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        KeyValueBytesStoreSupplier clientsStoreSupplier = Stores.inMemoryKeyValueStore("clients-store");
        
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Client> clientAvroSerde = new SpecificAvroSerde<>();
        
        final boolean isKeySerde = false;
        clientAvroSerde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
                        "http://localhost:8081"),
                isKeySerde);

        StreamsBuilder builder = new StreamsBuilder(); 
        
        KTable<String,Client> clientsKTable = builder.table(
                "clients", // The topic name
                Materialized.<String,Client>as(clientsStoreSupplier)
                        .withKeySerde(stringSerde)
                        .withValueSerde(clientAvroSerde)
                        .withCachingDisabled());
        
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfigProperties);
        streams.setUncaughtExceptionHandler(new ExceptionHandler());
        streams.start();
        
        try{
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException ex){
            log.error("{}", ex, ex);
        }
        
        log.info("clientsKTable.queryableStoreName(): {}", clientsKTable.queryableStoreName());
        
        ReadOnlyKeyValueStore<String, Client> clientsStore = streams.store(clientsKTable.queryableStoreName(), 
                QueryableStoreTypes.<String, Client>keyValueStore());
        
        String clientId = "cf0c7b1fc443";
        Client client = clientsStore.get(clientId);
        
        log.info("Client retrieved from the local store and also from the topic: {}", client);
        
        try{
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException ex){
            log.error("{}", ex, ex);
        }
        
        log.info("Going to close the \"streams\"");
        streams.close();
        
        log.info("Exiting");
    }
}
