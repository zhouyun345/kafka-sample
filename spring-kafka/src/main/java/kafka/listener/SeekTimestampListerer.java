package kafka.listener;

import java.util.ArrayList;
import java.util.Map;
import kafka.model.Command;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

@Slf4j
//@Service
public class SeekTimestampListerer extends AbstractConsumerSeekAware {

  @KafkaListener(id = "seekTimestamp", topics = "topic1")
  public void listen(
      @Header(value = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) String key,
      Command command,
      @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp,
      @Header(KafkaHeaders.RAW_DATA) ConsumerRecord<String, byte[]> consumerRecord) {
    log.info("Received message timestamp: {} {} {}", timestamp, key, command);
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments,
      ConsumerSeekCallback callback) {
    long timestamp = 1681637700000L;
    log.info("Search for a time that is great or equal then {}", timestamp);
    callback.seekToTimestamp(new ArrayList<>(assignments.keySet()), timestamp);
  }
}
