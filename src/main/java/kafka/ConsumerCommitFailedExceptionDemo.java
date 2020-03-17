package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 * @author Administrator
 */
public class ConsumerCommitFailedExceptionDemo {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.put(BOOTSTRAP_SERVERS_CONFIG, Config.SERVERS);
        properties.put(GROUP_ID_CONFIG, "test");
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(REQUEST_TIMEOUT_MS_CONFIG, "30000");
        properties.put(SESSION_TIMEOUT_MS_CONFIG, "40000");
        properties.put(MAX_POLL_INTERVAL_MS_CONFIG, 2000);
        properties.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(MAX_POLL_RECORDS_CONFIG, 10);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(CONNECTIONS_MAX_IDLE_MS_CONFIG, 1000);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(Config.topic));
        while (true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(java.time.Duration.ofMillis(100));
            Thread.sleep(6000L);
            System.out.println("no data");
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }
}
