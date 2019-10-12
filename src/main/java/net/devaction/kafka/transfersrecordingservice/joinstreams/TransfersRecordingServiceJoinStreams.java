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
        implements TransfersRecordingService {

    private static final Logger log = LoggerFactory.getLogger(
            TransfersRecordingServiceJoinStreams.class);

    private KafkaStreams streams;

    private static final String ACCOUNT_BALANCES_TOPIC = "account-balances";

    private static final String TRANSFERS_TOPIC = "transfers";

    @Override
    public void start() {
        final ConfigValues configValues = readConfig();

        final Properties streamsConfigProperties =
                createStreamsConfigProperties(configValues.getBootstrapServers());

        final Serde<String> stringSerde = Serdes.String();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Transfer> transfersKStream =
                createInputKStream(stringSerde, configValues.getSchemaRegistryUrl(),
                builder);

        final Serde<AccountBalance> accountBalanceSerde =
                createAccountBalanceSerde(configValues.getSchemaRegistryUrl());

        final KTable<String,AccountBalance> accountBalancesKTable =
                createInputKTable(stringSerde, accountBalanceSerde, builder);

        final KStream<String, AccountBalance> newABstream = transfersKStream.join(
                accountBalancesKTable, new TransferAndAccountBalanceJoiner());

        newABstream.to(ACCOUNT_BALANCES_TOPIC, Produced.with(stringSerde, accountBalanceSerde));

        streams = new KafkaStreams(builder.build(), streamsConfigProperties);
        streams.setUncaughtExceptionHandler(new ExceptionHandler());

        startStreams();
    }

    @Override
    public void stop() {
        log.info("Going to close the \"Streams\"");
        streams.close();
    }

    private void startStreams() {
        streams.start();

        while (streams.state() != State.RUNNING) {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException ex) {
                log.error("Interrupted while waiting for the \"Streams\" to start.", ex);
                Thread.currentThread().interrupt();
            }
        }

        log.info("\"Streams\" started");
    }

    private ConfigValues readConfig() {
        ConfigValues configValues = null;

        log.info("Going to read the configuration values");
        try {
            configValues = new ConfigReader().read();
        } catch (Exception ex) {
            log.error("Unable to read the configuration values, exiting", ex);
            System.exit(1);
        }

        return configValues;
    }

    private Properties createStreamsConfigProperties(String bootstrapServers) {
        final Properties streamsConfigProperties = new Properties();

        streamsConfigProperties.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "account-balance-streams-retriever");

        streamsConfigProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);

        streamsConfigProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return streamsConfigProperties;
    }

    private KStream<String, Transfer> createInputKStream(Serde<String> stringSerde,
            String schemaRegistryUrl, StreamsBuilder builder) {

        final boolean isKeySerde = false;
        final Serde<Transfer> transferSerde = new SpecificAvroSerde<>();
        transferSerde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        schemaRegistryUrl),
                isKeySerde);

        return builder.stream(TRANSFERS_TOPIC,
                Consumed.with(stringSerde, transferSerde));
    }

    private KTable<String,AccountBalance> createInputKTable(
            Serde<String> stringSerde, Serde<AccountBalance> accountBalanceSerde,
            StreamsBuilder builder) {

        final KeyValueBytesStoreSupplier clientsStoreSupplier =
                Stores.inMemoryKeyValueStore("account-balance-store");

        return builder.table(
                ACCOUNT_BALANCES_TOPIC,
                Materialized.<String,AccountBalance>as(clientsStoreSupplier)
                        .withKeySerde(stringSerde)
                        .withValueSerde(accountBalanceSerde)
                        .withCachingDisabled());
    }

    private Serde<AccountBalance> createAccountBalanceSerde(String schemaRegistryUrl) {
        final Serde<AccountBalance> accountBalanceSerde = new SpecificAvroSerde<>();
        final boolean isKeySerde = false;
        accountBalanceSerde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        schemaRegistryUrl),
                isKeySerde);

        return accountBalanceSerde;
    }
}
