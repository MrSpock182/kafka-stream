package br.com.studiotrek.kafkastream.messaging;

import io.github.studiotrek.kafka.data.FooDetailsAvro;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class FooDetailsConsumerWithKafka {

    @RetryableTopic(
            autoCreateTopics = "false",
            backoff = @Backoff(delay = 15000, multiplier = 2.0),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
    @KafkaListener(
            topics = "${spring.kafka.topics.foo-details}",
            groupId = "${spring.kafka.groups.foo-details-group}"
    )
    public void listener(
            final @Header("hash") String hash,
            final @Header("version") String version,
            final @Header("user") String user,
            final @Header("cid") String cid,
            final @Payload ConsumerRecord<String, FooDetailsAvro> consumerRecord) {
        final StringBuilder sb = new StringBuilder();
        sb.append("-------- FORMULARIO DETALHADO --------\n");
        sb.append("HASH: ").append(hash).append("\n");
        sb.append("VERSION: ").append(version).append("\n");
        sb.append("USER: ").append(user).append("\n");
        sb.append("CID: ").append(cid).append("\n");
        sb.append("KEY: ").append(consumerRecord.key()).append("\n");
        sb.append("VALUE: ").append(consumerRecord.value()).append("\n");
        sb.append("TIME: ").append(LocalDateTime.now()).append("\n");
        System.out.println(sb);
    }
}