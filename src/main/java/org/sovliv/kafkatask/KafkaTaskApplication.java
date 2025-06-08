package org.sovliv.kafkatask;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class KafkaTaskApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaTaskApplication.class, args);
    }

}
