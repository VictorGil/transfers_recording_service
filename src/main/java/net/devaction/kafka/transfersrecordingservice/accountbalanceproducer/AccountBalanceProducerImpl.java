package net.devaction.kafka.transfersrecordingservice.accountbalanceproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.devaction.entity.AccountBalanceEntity;
import net.devaction.kafka.avro.AccountBalance;
import net.devaction.kafka.avro.util.AccountBalanceConverter;
import net.devaction.kafka.transfersrecordingservice.callback.SimpleProducerCallBack;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class AccountBalanceProducerImpl implements AccountBalanceProducer{
    private static final Logger log = LoggerFactory.getLogger(AccountBalanceProducerImpl.class);

    private KafkaProducer<String, AccountBalance> producer;

    private static final String ACCOUNT_BALANCES_TOPIC = "account-balances";

    private final SimpleProducerCallBack callBack = new SimpleProducerCallBack();

    @Override
    public void start(String bootstrapServers, String schemaRegistryUrl){
        final Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        producer = new KafkaProducer<String, AccountBalance>(props);
    }

    @Override
    public void send(AccountBalanceEntity accountBalanceEntity){
        log.info("Going to send/produce/publish the following account balance data: {}",
                accountBalanceEntity);
        AccountBalance accountBalance = AccountBalanceConverter.convertToAvro(accountBalanceEntity);

        final ProducerRecord<String, AccountBalance> record =
                new ProducerRecord<String, AccountBalance>(ACCOUNT_BALANCES_TOPIC, accountBalance.getAccountId(),
                accountBalance);

        producer.send(record, callBack);
    }

    @Override
    public void stop(){
        if (producer != null)
            producer.close();
    }
}
