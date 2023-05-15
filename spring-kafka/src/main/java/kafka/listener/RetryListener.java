package kafka.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import kafka.model.DelayMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class RetryListener {

  @Autowired
  private ObjectMapper objectMapper;

  @RetryableTopic(attempts = "2", backoff = @Backoff(delay = 10 * 1000))
  @KafkaListener(groupId = "testDelay", topics = "testDelay")
  public void listener(String message) throws JsonProcessingException {
    final DelayMessage delayMessage = objectMapper.readValue(message, DelayMessage.class);
    log.info("listen: {}", delayMessage);
    if (LocalDateTime.now().isAfter(delayMessage.getTime())) {
      log.info("start consume: {}", delayMessage);
    } else {
      throw new RuntimeException("error");
    }
  }
}
