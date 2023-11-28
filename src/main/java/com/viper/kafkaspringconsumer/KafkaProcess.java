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
public class KafkaProcess {

    private final InsightRepository insightRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
//    Matomo 수집을 통한 최조 Kafka 적재 데이터 예시
//    '{"key1": "key1-data1" ,"key2":"key2-data2", "key3":"key3-data3"}'

    @KafkaListener(topics = "my-topic", groupId = "my-group", concurrency = "3")
    public void listenAndParseData(ConsumerRecord<String, String> record) {

        // 최초 수집 데이터를 db에 저장
        final Insight insight = Insight.toEntity(record.key(), record.value());
        insightRepository.save(insight);
        try {
            List<KafkaRecord> kafkaRecordList = dataToKafkaRecordList(record.value());
            // 여기서 record를 파싱

            for(KafkaRecord kafkaRecord : kafkaRecordList){
                kafkaRecord.setValue(kafkaRecord.getValue()+ " - parsed");
                // 파싱한 데이터를 다시 Kafka에 전송
                kafkaTemplate.send("parsed-data-topic-test", kafkaRecord.getKey(), kafkaRecord.getValue());
            }

        }catch(IOException e){
            e.printStackTrace();
        }


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


    private List<KafkaRecord> saveToKeyValueStore(JsonNode jsonNode) {
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        List<KafkaRecord> kafkaRecordList = new ArrayList<>();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> topicEntry = fields.next();
            String key = topicEntry.getKey();
            String value = topicEntry.getValue().asText();
            kafkaRecordList.add(KafkaRecord.builder()
                    .key(key)
                    .value(value)
                    .build());
        }
        return kafkaRecordList;
    }

    private List<KafkaRecord> dataToKafkaRecordList(String jsonString) throws IOException {
        JsonNode jsonNode = jsonStringToJsonObject(jsonString);
        return saveToKeyValueStore(jsonNode);
    }

    private static JsonNode jsonStringToJsonObject(String jsonString) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readTree(jsonString);
    }
}
