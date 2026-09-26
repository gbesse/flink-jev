package io.github.gbesse.flink.jev;

import java.io.Serializable;
import java.net.URI;
import java.util.Set;

record JevSettings(String endpoint, String model, String question, double threshold,
                   int maxInputBytes, int timeoutMs, int maxCalls) implements Serializable {
  void validate() {
    URI uri = URI.create(endpoint);
    boolean local = "http".equals(uri.getScheme()) && Set.of("127.0.0.1", "localhost").contains(uri.getHost());
    if (!"https".equals(uri.getScheme()) && !local) throw new IllegalArgumentException("Jev endpoint must use HTTPS");
    if (model == null || model.isBlank() || question == null || question.isBlank()
        || !Double.isFinite(threshold) || threshold < 0.5 || threshold > 1
        || maxInputBytes < 1 || maxInputBytes > 32768
        || timeoutMs < 100 || timeoutMs > 120000
        || maxCalls < 1 || maxCalls > 1000000) throw new IllegalArgumentException("Invalid Jev model options");
  }
}
