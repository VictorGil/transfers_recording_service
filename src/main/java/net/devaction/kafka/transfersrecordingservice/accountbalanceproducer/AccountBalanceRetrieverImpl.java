package net.devaction.kafka.transfersrecordingservice.accountbalanceproducer;

import java.util.Collections;
import java.util.Properties;

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

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import net.devaction.entity.AccountBalanceEntity;
import net.devaction.kafka.avro.AccountBalance;
import net.devaction.kafka.avro.util.AccountBalanceConverter;
import net.devaction.kafka.streams.ExceptionHandler;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class AccountBalanceRetrieverImpl implements AccountBalanceRetriever{
    private static final Logger log = LoggerFactory.getLogger(AccountBalanceRetrieverImpl.class);

    private ReadOnlyKeyValueStore<String, AccountBalance> store;
    
    private KafkaStreams streams;

    @Override
    public void start(String bootstrapServers, String schemaRegistryUrl) {
        
        final Properties streamsConfigProperties = new Properties();
        streamsConfigProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "account-balance-retriever");
        streamsConfigProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfigProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        KeyValueBytesStoreSupplier clientsStoreSupplier = 
                Stores.inMemoryKeyValueStore("account-balance-store");
        
        final Serde<String> stringSerde = Serdes.String();
        final Serde<AccountBalance> accountBalanceSerde = new SpecificAvroSerde<>();
        
        final boolean isKeySerde = false;
        accountBalanceSerde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
                        schemaRegistryUrl),
                isKeySerde);

        StreamsBuilder builder = new StreamsBuilder(); 
        
        KTable<String,AccountBalance> clientsKTable = builder.table(
                "account-balances", // The topic name
                Materialized.<String,AccountBalance>as(clientsStoreSupplier)
                        .withKeySerde(stringSerde)
                        .withValueSerde(accountBalanceSerde)
                        .withCachingDisabled());
        
        streams = new KafkaStreams(builder.build(), streamsConfigProperties);
        streams.setUncaughtExceptionHandler(new ExceptionHandler());
        streams.start();       
                
        store = streams.store(clientsKTable.queryableStoreName(), 
                QueryableStoreTypes.<String,AccountBalance>keyValueStore());                
    }
    
    @Override
    public AccountBalanceEntity retrieve(String accountId){

        AccountBalance accountBalance = store.get(accountId);
        
        if (accountBalance == null)
            return null;
        
        return AccountBalanceConverter.convertToPojo(accountBalance);
    }
    
    @Override
    public void stop() {
        log.info("We have been told to stop, closing the \"Streams\"");
        streams.close();
    }
}
