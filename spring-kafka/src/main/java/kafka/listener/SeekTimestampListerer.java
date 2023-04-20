package kafka.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

/**
 * 从指定offset或timestamp回溯消费.
 */
@Slf4j
@Service
public class SeekTimestampListerer extends AbstractConsumerSeekAware {

  @KafkaListener(id = "seekTimestamp", topics = "topic1")
  public void listen(
      @Header(value = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) String key,
      @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp,
      @Header(KafkaHeaders.RAW_DATA) ConsumerRecord<String, String> consumerRecord,
      String data) {
    log.info("Received message timestamp => {}, key => {}, data => {}", timestamp, key, data);
  }

//  @Override
//  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments,
//      ConsumerSeekCallback callback) {
//    long timestamp = 1681637700000L;
//    log.info("Search for a time that is great or equal then {}", timestamp);
//    callback.seekToTimestamp(new ArrayList<>(assignments.keySet()), timestamp);
//  }

  /**
   * 回溯到开始位置.
   *
   * @param topic
   */
  public void rewindBeginning(String topic) {
    getSeekCallbacks()
        .forEach((tp, callback) -> {
          if (tp.topic().equals(topic)) {
            callback.seekToBeginning(tp.topic(), tp.partition());
          }
        });
  }

  /**
   * 回溯某个topic partition到指定位置.
   *
   * @param topic
   * @param partition
   */
  public void rewindOnePartitionBeginning(String topic, int partition) {
    getSeekCallbackFor(new TopicPartition(topic, partition))
        .seekToBeginning(topic, partition);
  }
}
