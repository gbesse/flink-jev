package io.github.gbesse.flink.jev;
import static org.junit.jupiter.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import org.apache.flink.table.data.*;
import org.junit.jupiter.api.Test;
class JevBodyDeadlineTest {
 @Test void deadlineIncludesAnUnfinishedBodyAndKeepsDigest() throws Exception {
  var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  var release=new CountDownLatch(1);
  server.createContext("/stall",e->{try{e.sendResponseHeaders(200,0);e.getResponseBody().write('{');e.getResponseBody().flush();release.await(5,TimeUnit.SECONDS);}catch(InterruptedException x){Thread.currentThread().interrupt();}finally{e.close();}});server.start();
  try {var f=new JevPredictFunction(new JevSettings("http://127.0.0.1:"+server.getAddress().getPort()+"/stall","jev-1.13.0","q",.8,4096,300,10));f.initForTest("synthetic");
   RowData row=f.asyncPredict(GenericRowData.of(StringData.fromString("sample"))).get(2,TimeUnit.SECONDS).iterator().next();
   assertTrue(row.isNullAt(0));assertEquals("failure",row.getString(1).toString());assertEquals(64,row.getString(2).toString().length());
  }finally{release.countDown();server.stop(0);}
 }
 @Test void oversizedBodyRejectedAndValidBodyStillRoutes() throws Exception {
  var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  server.createContext("/big",e->{try{byte[] b=new byte[100001];e.sendResponseHeaders(200,b.length);e.getResponseBody().write(b);}finally{e.close();}});
  server.createContext("/ok",e->{try{byte[] b="{\"model\":\"jev-1.13.0\",\"answers\":{\"decision\":{\"type\":\"noul\",\"noul\":0.9}}}".getBytes(java.nio.charset.StandardCharsets.UTF_8);e.sendResponseHeaders(200,b.length);e.getResponseBody().write(b);}finally{e.close();}});server.start();
  try {for(String path:new String[]{"big","ok"}){var f=new JevPredictFunction(new JevSettings("http://127.0.0.1:"+server.getAddress().getPort()+"/"+path,"jev-1.13.0","q",.8,4096,3000,10));f.initForTest("synthetic");var row=f.asyncPredict(GenericRowData.of(StringData.fromString("sample"))).get(5,TimeUnit.SECONDS).iterator().next();assertEquals(path.equals("ok")?"yes":"failure",row.getString(1).toString());}}
  finally{server.stop(0);}
 }
}
