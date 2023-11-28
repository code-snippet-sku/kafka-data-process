package com.viper.kafkaspringconsumer;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KafkaRecord {

    private String topic;
    private String key;
    private String value;

    @Builder
    private KafkaRecord(String topic, String key, String value){
        this.topic = topic;
        this.key = key;
        this.value = value;
    }
}
