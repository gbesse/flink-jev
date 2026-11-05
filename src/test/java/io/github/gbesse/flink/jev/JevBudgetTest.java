package io.github.gbesse.flink.jev;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.charset.StandardCharsets;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
class JevBudgetTest {
 @Test void exhaustedBudgetNeverSendsAgain() throws Exception {
  AtomicInteger received=new AtomicInteger();HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  server.createContext("/",exchange->{received.incrementAndGet();byte[] body="{\"model\":\"jev-1.13.0\",\"answers\":{\"decision\":{\"type\":\"noul\",\"noul\":0.9}}}".getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,body.length);try(var stream=exchange.getResponseBody()){stream.write(body);}});server.start();
  try {var f=new JevPredictFunction(new JevSettings("http://127.0.0.1:"+server.getAddress().getPort()+"/","jev-1.13.0","q",.8,1000,3000,2));f.initForTest("synthetic");
   for(int i=0;i<20;i++){var row=f.asyncPredict(GenericRowData.of(StringData.fromString("source"))).get().iterator().next();assertEquals(i<2?"yes":"review",row.getString(1).toString());}
   assertEquals(2,received.get());
  }finally{server.stop(0);}
 }
}
