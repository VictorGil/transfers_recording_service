package net.devaction.kafka.transfersrecordingservice.clientproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.devaction.entity.ClientEntity;
import net.devaction.kafka.avro.Client;
import net.devaction.kafka.avro.util.ClientConverter;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class ClientProducer{
    private static final Logger log = LoggerFactory.getLogger(ClientProducer.class);
    
    private KafkaProducer<String, Client> producer;
    
    private static final String CLIENTS_TOPIC = "clients";
    
    private final ClientProducerCallBack callBack = new ClientProducerCallBack();
    
    public void start(String bootstrapServers, String schemaRegistryUrl){
        final Properties props = new Properties();
        
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        
        producer = new KafkaProducer<String, Client>(props);
    }
    
    public void send(ClientEntity clientEntity){
        log.info("Going to send/produce/publish the following client data: {}", clientEntity);
        Client client = ClientConverter.convertToAvro(clientEntity);
        
        final ProducerRecord<String, Client> record = 
                new ProducerRecord<String, Client>(CLIENTS_TOPIC, client.getId(), 
                        client);
        producer.send(record, callBack);        
    }
    
    public void stop(){
        producer.close();
    }
}


