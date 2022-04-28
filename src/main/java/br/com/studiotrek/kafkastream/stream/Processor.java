package br.com.studiotrek.kafkastream.stream;

import io.github.studiotrek.kafka.data.FooDetailsAvro;
import io.github.studiotrek.kafka.data.FooSummaryAvro;
import io.github.studiotrek.kafka.data.ProductAvro;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.util.Collections;
import java.util.Map;

@Component
public class Processor {

    private final String schemaRegistryUrl;
    private final String fooDetails;
    private final String fooSummary;

    public Processor(
            @Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl,
            @Value("${spring.kafka.topics.foo-details}") String fooDetails,
            @Value("${spring.kafka.topics.foo-summary}") String fooSummary
    ) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.fooDetails = fooDetails;
        this.fooSummary = fooSummary;
    }

    @Autowired
    public void process(final StreamsBuilder builder) {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);
        final Serde<String> stringSerde = Serdes.String();

        final Serde<FooDetailsAvro> fooAvroV3Serde = new SpecificAvroSerde<>();
        fooAvroV3Serde.configure(serdeConfig, false);

        final Serde<FooSummaryAvro> fooSummaryAvroV1Serde = new SpecificAvroSerde<>();
        fooSummaryAvroV1Serde.configure(serdeConfig, false);

        KStream<String, FooDetailsAvro> stream = builder
                .stream(fooDetails, Consumed.with(stringSerde, fooAvroV3Serde));
        KStream<String, FooSummaryAvro> streamSummary = stream
                .mapValues(foSummaryAvro ->
                        getSummary(foSummaryAvro, foSummaryAvro
                                .getProducts()
                                .stream()
                                .mapToDouble(ProductAvro::getValue)
                                .sum()
                        )
                );
        streamSummary.to(fooSummary, Produced.with(stringSerde, fooSummaryAvroV1Serde));
    }

    private FooSummaryAvro getSummary(final FooDetailsAvro fooAvroV3, final Double valueSummary) {
        return FooSummaryAvro.newBuilder()
                .setId(fooAvroV3.getId())
                .setName(fooAvroV3.getName())
                .setDescription(fooAvroV3.getDescription())
                .setDate(fooAvroV3.getDate())
                .setProducts(valueSummary)
                .build();
    }
}