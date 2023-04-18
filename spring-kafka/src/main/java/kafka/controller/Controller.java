/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import kafka.model.Command;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class Controller {

  @Autowired
  private KafkaTemplate<Object, Object> kafkaTemplate;

  @Autowired
  private ConsumerFactory<Object, Object> consumerFactory;

  /**
   * 发送消息.
   *
   * @param str
   */
  @PostMapping(path = "/send/{str}")
  public void sendFoo(@PathVariable String str) {
    this.kafkaTemplate.send("topic1", new Command(str));
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
}
