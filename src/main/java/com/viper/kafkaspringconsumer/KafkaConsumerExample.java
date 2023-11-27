package com.viper.kafkaspringconsumer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaConsumerExample {

    final InsightRepository insightRepository;

    //카프카 데이터를 db에 적재하는 코드
    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void listen(ConsumerRecord<String, String> record) {

        System.out.println("토픽: " + record.topic() + ", 키: " + record.key() + ", 값: " + record.value());
        final Insight insight = Insight.toEntity(record.key(), record.value());
        insightRepository.save(insight);

    }
}
