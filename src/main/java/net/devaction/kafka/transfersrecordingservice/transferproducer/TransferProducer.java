package net.devaction.kafka.transfersrecordingservice.transferproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.devaction.entity.TransferEntity;

import net.devaction.kafka.avro.Transfer;
import net.devaction.kafka.avro.util.TransferConverter;
import net.devaction.kafka.transfersrecordingservice.callback.SimpleProducerCallBack;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TransferProducer {
    private static final Logger log = LoggerFactory.getLogger(TransferProducer.class);

    private KafkaProducer<String, Transfer> producer;

    private static final String TRANSFERS_TOPIC = "transfers";

    private final SimpleProducerCallBack callBack = new SimpleProducerCallBack();

    public void start(String bootstrapServers, String schemaRegistryUrl) {
        final Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "transfers-recording-service-transfer-producer-01");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        producer = new KafkaProducer<String, Transfer>(props);
    }

    public void send(TransferEntity transferEntity) {
        log.info("Going to send/produce/publish the following \"transfer\" data: {}", transferEntity);
        Transfer transfer = TransferConverter.convertToAvro(transferEntity);

        final ProducerRecord<String, Transfer> record =
                // Please note that the key must be the account id and not the transfer id
                new ProducerRecord<String, Transfer>(TRANSFERS_TOPIC, transfer.getAccountId(),
                        transfer);
        producer.send(record, callBack);
    }

    public void stop() {
        producer.close();
    }
}
