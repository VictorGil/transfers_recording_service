package net.devaction.kafka.transfersrecordingservice.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class ConfigValues{
    private static final Logger log = LoggerFactory.getLogger(ConfigValues.class);
    
    @JsonProperty("bootstrap_servers")
    private String bootstrapServers;
    
    @JsonProperty("schema_registry_URL")
    private String schemaRegistryUrl;
    
    @Override
    public String toString(){
        return "ConfigValues [bootstrapServers=" + bootstrapServers + ", schemaRegistryUrl=" + schemaRegistryUrl + "]";
    }

    public String getBootstrapServers(){
        return bootstrapServers;
    }
    
    public void setBootstrapServers(String bootstrapServers){
        this.bootstrapServers = bootstrapServers;
    }
    
    public String getSchemaRegistryUrl(){
        return schemaRegistryUrl;
    }
    
    public void setSchemaRegistryUrl(String schemaRegistryUrl){
        this.schemaRegistryUrl = schemaRegistryUrl;
    }    
}

