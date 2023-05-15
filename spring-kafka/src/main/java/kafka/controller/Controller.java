package kafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import kafka.listener.SeekTimestampListerer;
import kafka.model.Command;
import kafka.model.DelayMessage;
import kafka.model.Payment;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class Controller {

  @Autowired
  private KafkaTemplate<Object, Object> kafkaTemplate;

  @Autowired
  private ConsumerFactory<Object, Object> consumerFactory;

  @Autowired
  private SeekTimestampListerer seekTimestampListerer;

  @Autowired
  private KafkaListenerEndpointRegistry registry;

  @Autowired
  private ObjectMapper objectMapper;

  /**
   * 发送消息.
   *
   * @param str
   */
  @PostMapping(path = "/send/{topic}/{str}")
  public void sendMsg(@PathVariable String topic,
      @PathVariable String str) {
    this.kafkaTemplate.send(topic, new Command(str));
  }

  /**
   * 发送body json.
   *
   * @param topic
   * @param delayMessage
   */
  @PostMapping(path = "/sendBody/{topic}")
  public void sendJsonMsg(@PathVariable String topic,
      @RequestBody DelayMessage delayMessage) throws JsonProcessingException {
    delayMessage.setTime(LocalDateTime.now().plusSeconds(10));
    this.kafkaTemplate.send(topic, objectMapper.writeValueAsString(delayMessage));
  }

  /**
   * sendAvro格式数据.
   *
   * @param topic
   * @param id
   */
  @PostMapping(path = "/sendAvro/{topic}/{id}")
  public void sendAvro(@PathVariable String topic,
      @PathVariable String id) {
    this.kafkaTemplate.send(topic, new Payment(id, 100.00));
  }

  /**
   * 接收指定offset消息.
   *
   * @param offsets
   */
  @PostMapping(path = "/receive")
  public void receiveRecords(@RequestParam List<Long> offsets) {
    kafkaTemplate.setConsumerFactory(consumerFactory);
    List<TopicPartitionOffset> requested = new ArrayList<>();
    for (Long l : offsets) {
      requested.add(new TopicPartitionOffset("topic1", 0, l));
    }
    final ConsumerRecords<Object, Object> records = this.kafkaTemplate.receive(requested);
    for (Iterator<ConsumerRecord<Object, Object>> it = records.iterator(); it.hasNext(); ) {
      ConsumerRecord<Object, Object> record = it.next();
      log.info("record => {}", record.toString());
    }
  }

  /**
   * 暂停消费者.
   */
  @PostMapping(path = "/pause/{groupId}")
  public void pauseConsumer(@PathVariable String groupId) {
    registry.getListenerContainer(groupId).pause();
  }

  /**
   * 还原消费者.
   */
  @PostMapping(path = "/resume/{groupId}")
  public void resumeConsumer(@PathVariable String groupId) {
    registry.getListenerContainer(groupId).resume();
  }

  /**
   * 回溯消费.
   *
   * @param topic
   */
  @PostMapping(path = "/rewind/{topic}")
  public void rewindAllBeginning(@PathVariable String topic) {
    seekTimestampListerer.rewindBeginning(topic);
  }
}
