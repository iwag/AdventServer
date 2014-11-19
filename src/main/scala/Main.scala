package com.github.iwag

import java.net.InetSocketAddress
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.server.TwitterServer
import com.twitter.finagle.{Thrift, Service, Http}
import com.twitter.finagle.http.HttpMuxer
import com.twitter.io.Charsets._
import com.twitter.logging.Logger
import com.twitter.util.{Time, Future, Await}
import org.apache.thrift.protocol.TBinaryProtocol
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.handler.codec.http._
import org.elasticsearch.thrift._

class HTTPServerImpl(log:Logger) extends Service[HttpRequest, HttpResponse]{

  private[this] lazy val cacheClient = {
    val s = ClientBuilder().hosts(new InetSocketAddress("localhost", 49090))
      .codec(ThriftClientFramedCodec()).hostConnectionLimit(1).build()
    new CacheService.FinagledClient(s, new TBinaryProtocol.Factory())
  }

  private[this] lazy val esClient =
    Thrift.client.withProtocolFactory(new TBinaryProtocol.Factory()).newIface[Rest.FutureIface]("localhost:49090")

  override def apply(request: HttpRequest): Future[HttpResponse] = {
    log.info(s"Received at ${Time.now} ${request}")

    val cacheResp:Future[CacheResponse] = cacheClient.get(request.getUri)

    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

    cacheResp.flatMap(
      res => {
        val s = res.hit match {
          case Some(value) => value
          case None => "not found"
        }
        response.setContent(copiedBuffer(s, Utf8))
        Future.value(response)
      }
    )
  }
}

object HTTPServer extends TwitterServer {

  val httpMux = new HttpMuxer().withHandler("/", new HTTPServerImpl(log))

  val httpServer = Http.serve(":48080", httpMux)

  def main() {

    onExit {
      adminHttpServer.close()
      httpServer.close()
    }

    log.info("start admin:"+adminHttpServer.boundAddress)
    log.info("start http:"+httpServer.boundAddress)
    Await.all(adminHttpServer,httpServer)
  }
}
