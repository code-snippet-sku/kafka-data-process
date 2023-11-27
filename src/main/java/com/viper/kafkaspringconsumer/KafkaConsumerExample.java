package com.viper.kafkaspringconsumer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaConsumerExample {

    private final InsightRepository insightRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    //카프카 데이터를 db에 적재하는 코드
//    @KafkaListener(topics = "my-topic", groupId = "my-group")
//    public void listen(ConsumerRecord<String, String> record) {
//
//        System.out.println("토픽: " + record.topic() + ", 키: " + record.key() + ", 값: " + record.value());
//        final Insight insight = Insight.toEntity(record.key(), record.value());
//        insightRepository.save(insight);
//
//    }

    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void listen(ConsumerRecord<String, String> record) {

        System.out.println("토픽: " + record.topic() + ", 키: " + record.key() + ", 값: " + record.value());

        // 여기서 record를 가공하고 필요한 형태로 변환 (예: 내용 수정, 필드 추가 등)
        String modifiedValue = record.value() + " - Processed";

        // 가공된 데이터를 다시 Kafka에 전송
        kafkaTemplate.send("processed-topic", record.key(), modifiedValue);

        // 가공된 데이터를 저장하거나 다른 작업을 수행
        final Insight insight = Insight.toEntity(record.key(), modifiedValue);
        insightRepository.save(insight);

    }
    


}
