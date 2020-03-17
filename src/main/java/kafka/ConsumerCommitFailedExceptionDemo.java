package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author Administrator
 */
public class ConsumerCommitFailedExceptionDemo {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put("enable.auto.commit", false);
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("request.timeout.ms", "30000");
        properties.put("session.timeout.ms", "40000");
        properties.put("max.poll.interval.ms", 5000);
        properties.put("isolation.level", "read_committed");
        properties.put("max.poll.records", 10);
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("connection.max.idle.ms", 1000);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(Config.topic));
        while (true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(java.time.Duration.ofMillis(1000));
            Thread.sleep(6000L);
            System.out.println("no data");
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }
}
