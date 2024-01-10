package ru.didcvee.kafkareactive.sec02;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import ru.didcvee.kafkareactive.sec01.Lec01KafkaConsumer;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    public static void main(String[] args) {
        var config = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var options = SenderOptions.<String, String>create(config);

        var flux = Flux.interval(Duration.ofMillis(100))
                .take(100)
                .map(i -> new ProducerRecord<>("aloha", i.toString(), "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        KafkaSender.create(options)
                .send(flux)
                .doOnNext(r -> log.info("correlation id: {}", r.correlationMetadata()))
                .subscribe();
    }
}
