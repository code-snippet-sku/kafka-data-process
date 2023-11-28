package com.viper.kafkaspringconsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    @KafkaListener(topics = "my-topic", groupId = "my-group", concurrency = "3")
    public void listenAndParseData(ConsumerRecord<String, String> record) {

//        System.out.println("토픽: " + record.topic() + ", 키: " + record.key() + ", 값: " + record.value());
        // 데이터를 db에 저장
        final Insight insight = Insight.toEntity(record.key(), record.value());
        insightRepository.save(insight);
        // 여기서 record를 파싱
        String parsedValue = record.value() + " - parsed";
        // 파싱한 데이터를 다시 Kafka에 전송
        kafkaTemplate.send("parsed-data-topic-test", record.key(), parsedValue);

    }

    @KafkaListener(topics = "parsed-data-topic-test", groupId = "my-group", concurrency = "3")
    public void filterData(ConsumerRecord<String, String> record) {

//        System.out.println("토픽: " + record.topic() + ", 키: " + record.key() + ", 값: " + record.value());

        // 데이터를 db에 저장
        final Insight insight = Insight.toEntity(record.key(), record.value());
        insightRepository.save(insight);

        // 여기서 record를 필터링
        String filteredValue = record.value() + " - filtered";

        // 필터링한 데이터를 다시 Kafka에 전송
        kafkaTemplate.send("filtered-data-topic-test", record.key(), filteredValue);

    }

    @KafkaListener(topics = "filtered-data-topic-test", groupId = "my-group", concurrency = "3")
    private void storeData(ConsumerRecord<String, String> record) {

//        System.out.println("토픽: " + record.topic() + ", 키: " + record.key() + ", 값: " + record.value());

        // 데이터를 db에 저장
        final Insight insight = Insight.toEntity(record.key(), record.value());
        insightRepository.save(insight);

        // 여기서 record를 최종 적재 전 처리
        String allDoneValue = record.value() + " - allDone";

        // 데이터를 db에 저장
        final Insight allDone = Insight.toEntity(record.key(), allDoneValue);
        insightRepository.save(allDone);


    }


}
