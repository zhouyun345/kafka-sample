package kafka.listener;

import kafka.model.Payment;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 消费topic1数据，消费失败重试两次，发送到topic1.DLT死信队列.
 */
@Slf4j
@Service
public class Listeners {

//  @KafkaListener(id = "commandGroup", topics = "topic1")
//  public void listen(String command) {
//    log.info("Received: {}", command);
////    if (command.getData().startsWith("fail")) {
//      throw new RuntimeException("failed");
////    }
//  }
//
//  @KafkaListener(id = "dltGroup", topics = "topic1.DLT")
//  public void dltListen(byte[] in) {
//    log.info("Received from DLT: {}", new String(in));
//  }

//  @KafkaListener(id = "finder", groupId = "sldiscn-finder-vds-1", topics = "FRA_slfinprovd_vehicledata_vehicle",
//  properties = {"schema.registry.url:https://schema-registry.pp-fra.messaging-kafka.aws.porsche-preview.cloud:8081"})
//  public void listenTest(String command) {
//    log.info("listenTest: {}", command);
//  }

  @KafkaListener(id = "avro", groupId = "avroGroup", topics = "avroTopic",
      properties = {"schema.registry.url:http://localhost:8081"})
  public void listenAvro(Payment payment, ConsumerRecord<?, ?> consumerRecord) {
    log.info("listenAvro: {}", payment);
    log.info("listenAvro consumerRecord: {}", consumerRecord);
    throw new RuntimeException("error");
  }
}
