package kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaConfig {

//  @Bean
  public DefaultErrorHandler errorHandler(KafkaOperations<Object, Object> template) {
    return new DefaultErrorHandler(
        new DeadLetterPublishingRecoverer(template), new FixedBackOff(1000L, 2));
  }

  @Bean
  public DefaultErrorHandler customErrorHandler() {
    ConsumerRecordRecoverer recoverer = new ConsumerRecordRecoverer() {
      @Override
      public void accept(ConsumerRecord<?, ?> consumerRecord, Exception e) {
        log.error("消费错误 record = {}", consumerRecord, e);
      }
    };

    DefaultErrorHandler errorHandler = new DefaultErrorHandler(
        recoverer, new FixedBackOff(1000L, 3));
    errorHandler.setAckAfterHandle(false);
    return errorHandler;
  }

//  @Bean
  public RecordMessageConverter converter() {
    return new JsonMessageConverter();
  }

  @Bean
  public NewTopic topic() {
    return new NewTopic("topic1", 1, (short) 1);
  }

  @Bean
  public NewTopic dlt() {
    return new NewTopic("topic1.DLT", 1, (short) 1);
  }
}
