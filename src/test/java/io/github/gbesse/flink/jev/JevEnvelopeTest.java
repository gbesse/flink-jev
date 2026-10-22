package io.github.gbesse.flink.jev;
import static org.junit.jupiter.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;
class JevEnvelopeTest {
 @Test void ambiguousOrMisroutedEnvelopesNeverProduceProbability() throws Exception {
  for(String body:new String[]{"{\"model\":\"different\",\"answers\":{\"decision\":{\"type\":\"noul\",\"noul\":0.99}}}",
"{\"answers\":{\"decision\":{\"type\":\"noul\",\"noul\":0.99}}}",
"{\"model\":\"jev-1.13.0\",\"answers\":{\"decision\":{\"type\":\"noul\",\"noul\":0.99},\"extra\":{\"type\":\"noul\",\"noul\":0.99}}}",
"{\"model\":\"jev-1.13.0\",\"model\":\"different\",\"answers\":{\"decision\":{\"type\":\"noul\",\"noul\":0.99}}}",
"{\"model\":\"jev-1.13.0\",\"answers\":{\"decision\":{\"type\":\"noul\",\"noul\":0.99}}} {}"}){
   HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
   server.createContext("/v1/systemone",exchange->{byte[] bytes=body.getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,bytes.length);try(var out=exchange.getResponseBody()){out.write(bytes);}});server.start();
   try{
    JevSettings settings=new JevSettings("http://127.0.0.1:"+server.getAddress().getPort()+"/v1/systemone","jev-1.13.0","Synthetic question",.8,4096,3000,10);
    JevPredictFunction f=new JevPredictFunction(settings);f.initForTest("synthetic");
    RowData r=f.asyncPredict(GenericRowData.of(StringData.fromString("synthetic text"))).get(5,TimeUnit.SECONDS).iterator().next();
    assertTrue(r.isNullAt(0));assertEquals("failure",r.getString(1).toString());
   }finally{server.stop(0);}
  }
 }
}
