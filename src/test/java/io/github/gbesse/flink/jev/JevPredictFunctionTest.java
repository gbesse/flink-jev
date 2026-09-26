package io.github.gbesse.flink.jev;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

class JevPredictFunctionTest {
  @Test void routesAsynchronouslyWithARealHttpResponse() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/v1/systemone", exchange -> {
      byte[] reply = "{\"answers\":{\"decision\":{\"type\":\"noul\",\"noul\":0.91}}}".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, reply.length);
      try (var stream = exchange.getResponseBody()) { stream.write(reply); }
    });
    server.start();
    try {
      JevSettings settings = new JevSettings("http://127.0.0.1:" + server.getAddress().getPort() + "/v1/systemone",
          "jev-1.13.0", "Does this review recommend the film?", 0.8, 4096, 3000, 10);
      JevPredictFunction function = new JevPredictFunction(settings);
      function.initForTest("test-key");
      RowData result = function.asyncPredict(GenericRowData.of(StringData.fromString("Wonderful film")))
          .get().iterator().next();
      assertEquals(0.91, result.getDouble(0));
      assertEquals("yes", result.getString(1).toString());
      assertEquals(64, result.getString(2).toString().length());
    } finally { server.stop(0); }
  }
}
