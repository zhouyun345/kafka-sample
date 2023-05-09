package cn.zy.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

//  @Bean
  public DefaultErrorHandler errorHandler(KafkaOperations<Object, Object> template) {
    return new DefaultErrorHandler(
        null, new FixedBackOff(1000L, 2));
  }

  @Bean
  public RecordMessageConverter converter() {
    return new JsonMessageConverter();
  }

  @Bean
  public BatchMessagingMessageConverter batchConverter() {
    return new BatchMessagingMessageConverter(converter());
  }

  @Bean
  public NewTopic topic2() {
    return TopicBuilder.name("topic2").partitions(1).replicas(1).build();
  }

  @Bean
  public NewTopic topic3() {
    return TopicBuilder.name("topic3").partitions(1).replicas(1).build();
  }

}
