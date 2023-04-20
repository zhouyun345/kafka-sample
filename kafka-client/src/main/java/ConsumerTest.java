import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * kafka-clients消费者.
 */
@Slf4j
public class ConsumerTest {

  public static void main(String[] args) {
    Properties properties = new Properties();

    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // 消费分组名
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
    // 是否自动提交offset
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    // 自动提交offset的间隔时间
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    // 心跳时间，服务端broker通过心跳确认consumer是否故障，如果发现故障，就会通过心跳下发
    // rebalance的指令给其他的consumer通知他们进行rebalance操作，这个时间可以稍微短一点
    properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000);
    // 服务端broker多久感知不到一个consumer心跳就认为他故障了，默认是10秒
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10 * 1000);
    // 如果两次poll操作间隔超过了这个时间，broker就会认为这个consumer处理能力太弱,会将其踢出消费组，将分区分配给别的consumer消费
    properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30 * 1000);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());

    try (var consumer = new KafkaConsumer<String, String>(properties)) {
      // 消费主题
      String topicName = "kafka-test";
      consumer.subscribe(Arrays.asList(topicName));
      // 消费指定分区
      // consumer.assign(Arrays.asList(new TopicPartition(topicName, 0)));
      // 消息回溯消费
      consumer.assign(Arrays.asList(new TopicPartition(topicName, 0)));
      consumer.seekToBeginning(Arrays.asList(new TopicPartition(topicName, 0)));
      //指定offset消费
      // consumer.seek(new TopicPartition(topicName, 0), 10);

      //poll() API 是拉取消息的长轮询，主要是判断consumer是否还活着，只要我们持续调用poll()，
      //消费者就会存活在自己所在的group中，并且持续的消费指定partition的消息。
      //底层是这么做的：消费者向server持续发送心跳，如果一个时间段（session.timeout.ms）consumer挂掉或是不能发送心跳，这个消费者会被认为是挂掉了，
      //这个Partition也会被重新分配给其他consumer
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        for (ConsumerRecord<String, String> record : records) {
          log.info("收到消息：offset = {}, key = {}, value = {}", record.offset(), record.key(),
              record.value());
        }
        if (records.count() > 0) {
          // 提交offset
          consumer.commitSync();
        }
      }
    }
  }
}
