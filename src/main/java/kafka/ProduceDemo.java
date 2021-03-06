package kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author Administrator
 */
public class ProduceDemo {

  private static KafkaProducer<String, String> producer;

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS_CONFIG, Config.SERVERS);
    props.put(ACKS_CONFIG, "all");
    props.put(RETRIES_CONFIG, 0);
    props.put(BATCH_SIZE_CONFIG, 5);
    props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producer = new KafkaProducer<>(props);

    for (int i = 0; i < 10; i++) {
      String messageStr = "你好，这是第" + i + "条数据";
      producer.send(new ProducerRecord<>(Config.topic, "Message", messageStr));
      System.out.println(messageStr);
    }

    producer.close();
  }

}
