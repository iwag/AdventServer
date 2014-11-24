package com.github.iwag

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.filter.LogFormatter
import com.twitter.finagle.filter.LoggingFilter
import com.twitter.finagle.service.{RetryPolicy, RetryingFilter}
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.server.TwitterServer
import com.twitter.finagle.{SimpleFilter, Thrift, Service, Http}
import com.twitter.finagle.http.HttpMuxer
import com.twitter.io.Charsets._
import com.twitter.logging.Logger
import com.twitter.util._
import org.apache.thrift.protocol.TBinaryProtocol
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.handler.codec.http._
import org.elasticsearch.thrift._

class CacheFilter(cacheAddr: InetSocketAddress) extends SimpleFilter[HttpRequest, HttpResponse] {
  private[this] lazy val cacheClient = Thrift.client.withProtocolFactory(new TBinaryProtocol.Factory()).newIface[CacheService.FutureIface](cacheAddr.toString) // FIXME

  override def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]): Future[HttpResponse] = {
    cacheClient.get(request.getUri) flatMap {
      _.hit match {
        case Some(v) => {
          val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
          response.setContent(copiedBuffer(v, Utf8))
          Future(response)
        }
        case _ => service(request)
      }
    }
  }
}

class ESService(esAddr: InetSocketAddress) extends Service[HttpRequest, HttpResponse] {

  private[this] lazy val esClient =
    Thrift.client.withProtocolFactory(new TBinaryProtocol.Factory()).newIface[Rest.FutureIface](esAddr.toString) // FIXME

  override def apply(request: HttpRequest): Future[HttpResponse] = {
    val es = esClient.execute(RestRequest(Method.Post, "/user/item/_search", None, None, Some(ByteBuffer.wrap("test".getBytes))))
    es map { res =>
      val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      response.setContent(copiedBuffer(res.body.get))
      response
    }
  }
}

class HTTPLoggingFilter(l: Logger) extends LoggingFilter[HttpRequest, HttpResponse] {
  override val log: Logger = l
  override val formatter: LogFormatter[HttpRequest, HttpResponse] = new LogFormatter[HttpRequest, HttpResponse] {
    override def format(request: HttpRequest, reply: HttpResponse, replyTime: Duration): String = ???

    override def formatException(request: HttpRequest, throwable: Throwable, replyTime: Duration): String = ???
  }
}

class HTTPServerImpl(log: Logger, esAddr: InetSocketAddress, cacheAddr: InetSocketAddress) extends Service[HttpRequest, HttpResponse] {

  val cacheFilter = new CacheFilter(cacheAddr)
  val esService = new ESService(esAddr)

  val retryFilter = new RetryingFilter[HttpRequest, HttpResponse](RetryPolicy.tries(2), new MockTimer)
  val logFilter = new HTTPLoggingFilter(log)

  val comp = logFilter andThen cacheFilter andThen retryFilter andThen esService

  private[this] lazy val cacheClient = {
    val s = ClientBuilder().hosts(cacheAddr)
      .codec(ThriftClientFramedCodec()).hostConnectionLimit(1).build()
    new CacheService.FinagledClient(s, new TBinaryProtocol.Factory())
  }

  private[this] lazy val esClient =
    Thrift.client.withProtocolFactory(new TBinaryProtocol.Factory()).newIface[Rest.FutureIface](esAddr.toString) // FIXME

  override def apply(request: HttpRequest): Future[HttpResponse] = comp(request)
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
