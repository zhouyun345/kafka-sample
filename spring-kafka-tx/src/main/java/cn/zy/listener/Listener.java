package cn.zy.listener;

import cn.zy.model.Foo2;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Listener {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @KafkaListener(id = "fooGroup2", topics = "topic2")
  public void listen1(List<Foo2> foos) throws IOException {
    log.info("fooGroup2 Received: " + foos);
    foos.forEach(f -> kafkaTemplate.send("topic3", f.getFoo().toUpperCase()));
//    log.info("Messages sent, hit Enter to commit tx");
//    System.in.read();
    throw new RuntimeException("test");
  }

  @KafkaListener(id = "fooGroup3", topics = "topic3")
  public void listen2(List<String> in) {
    log.info("fooGroup3 Received: " + in);
  }
}
