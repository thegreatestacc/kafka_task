package org.sovliv.kafkatask.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import static org.sovliv.kafkatask.utils.Constants.DATA_TOPIC;

/**
 * @author Vladimir Solovyov
 * @project KafkaTask
 * @date on 31/05/2025
 */

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic bulkTopic() {
        return TopicBuilder.name(DATA_TOPIC)
                .partitions(12)
                .replicas(3)
                .build();
    }
}

