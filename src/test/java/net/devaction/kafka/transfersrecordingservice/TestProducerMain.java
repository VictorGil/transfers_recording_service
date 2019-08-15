package net.devaction.kafka.transfersrecordingservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import net.devaction.entity.ClientEntity;

import net.devaction.kafka.avro.Client;
import net.devaction.kafka.avro.util.ClientConverter;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class TestProducerMain{
    private static final Logger log = LoggerFactory.getLogger(TestProducerMain.class);
    
    
    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        ClientEntity clientEntity = new ClientEntity();
        clientEntity.generateId();
        clientEntity.setFirstName("James");
        clientEntity.setLastName("Jackson");
        clientEntity.setEmail("j.jackson@gmx.com");
        clientEntity.setAddress("Orange Street 22");
        clientEntity.setLevel("bronze");
        
        log.info("Going to send/produce/publish the following \"client\" data: {}", clientEntity);
        
        Client client = ClientConverter.convertToAvro(clientEntity);
                
        KafkaProducer<String, Client> producer = new KafkaProducer<String, Client>(props);
        
        final ProducerRecord<String, Client> record = 
                new ProducerRecord<String, Client>("clients", client.getId(), 
                        client);
        
        producer.send(record, new TestProducerCallBack());
        
        log.info("Sleeping while the message is sent");
        try{
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException ex){
            log.error(ex.toString(), ex);
        }
        
        producer.close();
    }
}
