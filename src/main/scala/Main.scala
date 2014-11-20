package com.github.iwag

import java.net.InetSocketAddress
import java.nio.ByteBuffer
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

class HTTPServerImpl(log:Logger, esAddr : InetSocketAddress, cacheAddr: InetSocketAddress) extends Service[HttpRequest, HttpResponse]{

  private[this] lazy val cacheClient = {
    val s = ClientBuilder().hosts(cacheAddr)
      .codec(ThriftClientFramedCodec()).hostConnectionLimit(1).build()
    new CacheService.FinagledClient(s, new TBinaryProtocol.Factory())
  }

  private[this] lazy val esClient =
    Thrift.client.withProtocolFactory(new TBinaryProtocol.Factory()).newIface[Rest.FutureIface](esAddr.toString) // FIXME

  override def apply(request: HttpRequest): Future[HttpResponse] = {
    log.info(s"Received at ${Time.now} ${request}")

    val cacheResp:Future[CacheResponse] = cacheClient.get(request.getUri)

    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

    cacheResp.flatMap(res => res.hit match {
      case Some(v) => Future(v)
      case _ => {
        val es = esClient.execute(RestRequest(Method.Post, "/user/item/_search", None, None, Some(ByteBuffer.wrap("test".getBytes))))
        es map (_.body.get.asCharBuffer().toString)
      }
    }).map { str =>
      val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      response.setContent(copiedBuffer(str, Utf8))
      response
    }
  }
}

object HTTPServer extends TwitterServer {
  val httpAddr = flag("http", new InetSocketAddress(0), "HTTP bind address")
  val esAddr = flag("es-addr", new InetSocketAddress(0), "Elasticsearch thrift address")
  val cacheAddr = flag("cache-addr", new InetSocketAddress(0), "Cache thrift address")

  val httpMux = new HttpMuxer().withHandler("/", new HTTPServerImpl(log, esAddr(), cacheAddr()))

  val httpServer = Http.serve(httpAddr(), httpMux)

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
