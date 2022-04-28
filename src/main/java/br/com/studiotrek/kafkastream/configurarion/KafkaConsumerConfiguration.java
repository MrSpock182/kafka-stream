package br.com.studiotrek.kafkastream.configurarion;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.github.studiotrek.kafka.data.FooDetailsAvro;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfiguration {

    private final String bootstrap;
    private final String groupId;
    private final String urlSchema;

    public KafkaConsumerConfiguration(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrap,
            @Value("${spring.kafka.groups.foo-details-group}") String groupId,
            @Value("${spring.kafka.properties.schema.registry.url}") String urlSchema) {
        this.bootstrap = bootstrap;
        this.groupId = groupId;
        this.urlSchema = urlSchema;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FooDetailsAvro> fooAvroV3FilterByNameFactory() {
        final ConcurrentKafkaListenerContainerFactory<String, FooDetailsAvro> factory
                = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setRecordFilterStrategy(recordFilterByNameStrategy());
        return factory;
    }

    private ConsumerFactory<String, FooDetailsAvro> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url", urlSchema);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private RecordFilterStrategy<? super String, ? super FooDetailsAvro> recordFilterByNameStrategy() {
        return consumerRecord -> {
            String name = consumerRecord.value().getName().toString();
            if (!name.equals("Genor")
                    && !name.equals("Kleber")
                    && !name.equals("Ronaldo")) {
                // SE TRUE NÃO EXECUTA
                return true;
            }
            return false;
        };
    }
}
