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

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class JoinStreamsTesterMain{
    private static final Logger log = LoggerFactory.getLogger(JoinStreamsTesterMain.class);

    public static void main(String[] args){
        new JoinStreamsTesterMain().run();
    }

    private void run() {
        final Properties streamsConfigProperties = new Properties();
        streamsConfigProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "account-balance-retriever");
        streamsConfigProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfigProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KeyValueBytesStoreSupplier clientsStoreSupplier =
                Stores.inMemoryKeyValueStore("account-balance-store");

        final Serde<String> stringSerde = Serdes.String();
        final Serde<AccountBalance> accountBalanceSerde = new SpecificAvroSerde<>();

        final boolean isKeySerde = false;
        accountBalanceSerde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        "http://localhost:8081"),
                isKeySerde);

        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String,AccountBalance> accountBalancesKTable = builder.table(
                "account-balances", // The topic name
                Materialized.<String,AccountBalance>as(clientsStoreSupplier)
                        .withKeySerde(stringSerde)
                        .withValueSerde(accountBalanceSerde)
                        .withCachingDisabled());

        final Serde<Transfer> transferSerde = new SpecificAvroSerde<>();
        transferSerde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        "http://localhost:8081"),
                isKeySerde);

        final KStream<String, Transfer> transfersKStream = builder.stream("transfers",
                Consumed.with(stringSerde, transferSerde));

        final KStream<String, AccountBalance> newABstream = transfersKStream.leftJoin(
                accountBalancesKTable, new TransferAndAccountBalanceJoiner());

        newABstream.to("account-balances", Produced.with(stringSerde, accountBalanceSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfigProperties);
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

        try{
            TimeUnit.MINUTES.sleep(5);
        } catch (InterruptedException ex){
            log.error("{}", ex, ex);
            Thread.currentThread().interrupt();
        }

        log.info("Going to close the \"Streams\"");
        streams.close();
    }
}
