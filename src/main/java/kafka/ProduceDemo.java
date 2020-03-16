package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Administrator
 */
public class ProduceDemo {
    private static KafkaProducer<String, String> producer;
    private static String topic = "test";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.SERVERS);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 1000; i++) {
            String messageStr = "你好，这是第" + i + "条数据";
            producer.send(new ProducerRecord<String, String>(topic, "Message", messageStr));
            System.out.println(messageStr);
        }

        producer.close();
    }

}
