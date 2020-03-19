package kafka.stream;

public class LogLine {

  private String payload;
  private Object schema;

  public String getPayload() {
    return payload;
  }
}