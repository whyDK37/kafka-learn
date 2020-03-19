package kafka.stream;

import com.google.gson.Gson;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import kafka.Config;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;

public class OSCheckStreamDemo {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "os-check-streams");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVERS);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS,
        Serdes.StringSerde.class.getName());
    final Gson gson = new Gson();
    final StreamsBuilder builder = new StreamsBuilder();
    // 元数据 topic
    KStream<String, String> source = builder.stream("access_log");
    source.mapValues(value -> gson.fromJson(value, LogLine.class)).mapValues(LogLine::getPayload)
        // 分组
        .groupBy((key, value) -> value.contains("ios") ? "ios" : "android")
        .windowedBy(TimeWindows.of(Duration.ofSeconds(2L)))
        .count()
        .toStream()
        // 目标 topic
        .to("os-check",
            Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.Long()));
    final Topology topology = builder.build();
    final KafkaStreams streams = new KafkaStreams(topology, props);
    final CountDownLatch latch = new CountDownLatch(1);

    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });
  }


}
