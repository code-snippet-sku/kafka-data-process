package com.viper.kafkaspringconsumer;


import jakarta.persistence.*;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

@Entity
@Table(name="insight")
@EntityListeners(AuditingEntityListener.class)
@NoArgsConstructor
@ToString
@Getter
public class Insight {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(updatable = false)
    protected Long id;

    @Column(name="key", nullable = true)
    private String key;

    @Column(name="value", nullable = false)
    private String value;

    @CreatedDate
    @Column(name="reg_dt")
    private LocalDateTime regDt;

    @Builder
    private Insight(String key, String value){

        this.key = key;
        this.value = value;

    }

    public static Insight toEntity(String key, String value){
        return Insight.builder()
                .key(key)
                .value(value)
                .build();
    }



}
