package kafka.listener;

import kafka.model.Command;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Listeners {

  @KafkaListener(id = "fooGroup", topics = "topic1")
  public void listen(Command command) {
    log.info("Received: {}", command);
    if (command.getData().startsWith("fail")) {
      throw new RuntimeException("failed");
    }
  }

  @KafkaListener(id = "dltGroup", topics = "topic1.DLT")
  public void dltListen(byte[] in) {
    log.info("Received from DLT: {}", new String(in));
  }
}
