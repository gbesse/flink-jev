package io.github.gbesse.flink.jev;
import static org.junit.jupiter.api.Assertions.*;import org.junit.jupiter.api.Test;import com.sun.net.httpserver.HttpServer;import java.net.InetSocketAddress;import java.util.concurrent.atomic.AtomicInteger;import java.nio.charset.StandardCharsets;import org.apache.flink.table.data.GenericRowData;import org.apache.flink.table.data.StringData;import org.apache.flink.types.RowKind;
class JevChangelogTest{
 @Test void nonInsertRowsNeverCallProviderOrConsumeBudget()throws Exception{
 AtomicInteger calls=new AtomicInteger();HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);server.createContext("/",exchange->{calls.incrementAndGet();byte[] body="{\"model\":\"jev-1.13.0\",\"answers\":{\"decision\":{\"type\":\"noul\",\"noul\":0.9}}}".getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,body.length);try(var out=exchange.getResponseBody()){out.write(body);}});server.start();
 try{var function=new JevPredictFunction(new JevSettings("http://127.0.0.1:"+server.getAddress().getPort()+"/","jev-1.13.0","q",.8,1000,3000,1));function.initForTest("synthetic");
 for(var kind:new RowKind[]{RowKind.DELETE,RowKind.UPDATE_BEFORE,RowKind.UPDATE_AFTER}){var row=GenericRowData.of(StringData.fromString("fixture"));row.setRowKind(kind);assertEquals("review",function.asyncPredict(row).get().iterator().next().getString(1).toString());}assertEquals(0,calls.get());assertEquals("yes",function.asyncPredict(GenericRowData.of(StringData.fromString("fixture"))).get().iterator().next().getString(1).toString());assertEquals(1,calls.get());
 }finally{server.stop(0);}}
}
