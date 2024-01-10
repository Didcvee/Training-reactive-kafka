package ru.didcvee.kafkareactive.sec01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/*
* Настройка auto.commit.interval.ms в Kafka отвечает за
* интервал времени, через который потребитель автоматически
* фиксирует свой текущий оффсет (offset) в группе потребителей (consumer group).
 *
*  */

public class Lec01KafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(Lec01KafkaConsumer.class);
    public static void main(String[] args) {
        var config = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var options = ReceiverOptions.create(config)
                .subscription(List.of("aloha"));

        KafkaReceiver
                .create(options)
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge()) // кафка запоминает оффсет консюмера,
                // на котором он остановился, из - за этой настройки получаем только обработанные сообщения
                .subscribe();


    }
}
