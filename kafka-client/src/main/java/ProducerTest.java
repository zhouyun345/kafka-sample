import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * kafka‐clients生产者.
 */
@Slf4j
public class ProducerTest {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    /**
     * 发出消息持久化机制参数
     * 1）acks=0： 表示producer不需要等待任何broker确认收到消息的回复，就可以继续发送下一条消息。性能最高，但是最容易丢消息。
     * 2）acks=1： 至少要等待leader已经成功将数据写入本地log，但是不需要等待所有follower是否成功写入。就可以继续发送下一条消息。
     *   这种情况下，如果follower没有成功备份数据，而此时leader又挂掉，则消息会丢失。
     * 3）acks=‐1或all： 这意味着leader需要等待所有备份(min.insync.replicas配置的备份个数)都成功写入日志，
     *   这种策略会保证只要有 一个备份存活就不会丢失数据。
     *   这是最强的数据保证。一般除非是金融级别，或跟钱打交道的场景才会使用这种配置。
     */
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    //把发送的key从字符串序列化为字节数组
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //把发送消息value从字符串序列化为字节数组
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Producer<String, String> producer = new KafkaProducer<>(properties);

    for (int i = 1; i <= 5; i++) {
      //指定发送分区
      String message = "this is msg" + i;
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
          "kafka-test", 0, String.valueOf(i), message);
      //未指定发送分区，具体发送的分区计算公式：hash(key)%partitionNum
      //ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("my‐replicated‐topic", order.getOrderId().toString(), JSON.toJSONString(order));
      // 等待消息发送成功的同步阻塞方法
//      RecordMetadata metadata = producer.send(producerRecord).get();
//      log.info("同步方式发送消息结果：topic => {}, partition => {}, offset => {}, message => {}",
//          metadata.topic(),
//          metadata.partition(), metadata.offset(), message);

      //异步方式发送消息
      producer.send(producerRecord, new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          if (exception != null) {
            log.error("发送消息失败", exception);
          }
          if (metadata != null) {
            log.info("异步方式发送消息结果：topic => {}, partition => {}, offset => {}, message => {}",
                metadata.topic(), metadata.partition(), metadata.offset(), message);
          }
        }
      });
    }

    producer.close();
  }
}
