package kafka.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.event.KafkaEvent;

/**
 * 支持暂停重启消费者.
 */
@Slf4j
//@Service
public class PauseResumeListener implements ApplicationListener<KafkaEvent> {

  //@KafkaListener(id = "pause.resume", topics = "pause.resume.topic")
  public void listen(String msg) {
    log.info("pause.resume.topic received: {}", msg);
  }

  @Override
  public void onApplicationEvent(KafkaEvent event) {
    log.info("pause.resume.topic event: {}", event);
  }

  @Bean
  public NewTopic pauseResumeTopic() {
    return TopicBuilder.name("pause.resume.topic")
        .partitions(2)
        .replicas(1)
        .build();
  }
}
