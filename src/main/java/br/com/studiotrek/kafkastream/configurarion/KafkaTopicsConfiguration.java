package br.com.studiotrek.kafkastream.configurarion;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicsConfiguration {

    private final String fooSummary;

    public KafkaTopicsConfiguration(@Value("${spring.kafka.topics.foo-summary}") String fooSummary) {
        this.fooSummary = fooSummary;
    }

    @Bean
    public NewTopic testStreamUpper() {
        return TopicBuilder
                .name(fooSummary)
                .partitions(3)
                .replicas(1)
                .compact()
                .configs(topicConfigs())
                .build();
    }

    private Map<String, String> topicConfigs() {
        String time = "86500000";
        Map<String, String> map = new HashMap<>();
        map.put("delete.retention.ms", time);
        map.put("file.delete.delay.ms", time);
        map.put("flush.messages", time);
        map.put("flush.ms", time);
        map.put("message.timestamp.difference.max.ms", time);
        map.put("retention.ms", time);
        map.put("segment.ms", time);
        map.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return map;
    }
}