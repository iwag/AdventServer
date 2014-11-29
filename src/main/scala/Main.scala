package com.github.iwag

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.filter.LogFormatter
import com.twitter.finagle.filter.LoggingFilter
import com.twitter.finagle.service.{RetryPolicy, RetryingFilter}
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.finagle.util.InetSocketAddressUtil
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

class ESService(log:Logger, esAddr: InetSocketAddress) extends Service[HttpRequest, HttpResponse] {

  private[this] lazy val esClient =
    Thrift.client.withProtocolFactory(new TBinaryProtocol.Factory()).newIface[Rest.FutureIface]("localhost:59090") // FIXME

  override def apply(request: HttpRequest): Future[HttpResponse] = {
    log.info(request.toString)

    val es = esClient.execute(RestRequest(Method.Get, "/java_river/status/_search?q=text:java", None, None, None))// Some(request.getContent.toByteBuffer)))
    es map { res =>
      log.info(res.toString)

      val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      response.setContent(copiedBuffer(res.body.get))
      response
    }
  }
}

class HTTPServerImpl(log: Logger, esAddr: InetSocketAddress, cacheAddr: InetSocketAddress) extends Service[HttpRequest, HttpResponse] {

  val cacheFilter = new CacheFilter(cacheAddr)
  val esService = new ESService(log, esAddr)


  val compositeService = /*cacheFilter andThen*/ esService

  def post(request :HttpRequest): Future[HttpResponse] = {
    val dbClient =    Thrift.client.withProtocolFactory(new TBinaryProtocol.Factory()).newIface[DbService.FutureIface]("localhost:59090") // FIXME
    val searchClient =
      Thrift.client.withProtocolFactory(new TBinaryProtocol.Factory()).newIface[SearchService.FutureIface]("localhost:59090") // FIXME
    val body = request.getContent.toString(Utf8)
    val stored = searchClient.put(body) flatMap {
      dbClient.put(_, body)
    }
    stored map { _ =>
      val response = new DefaultHttpResponse(request.getProtocolVersion, HttpResponseStatus.OK)
      response.setContent(copiedBuffer("OK", Utf8))
      response
    }
  }

  override def apply(request: HttpRequest): Future[HttpResponse] = {
    request.getMethod match {
      case HttpMethod.GET => compositeService(request)
      case HttpMethod.POST => post(request)
    }
  }

}

object HTTPServer extends TwitterServer {
  val httpAddr = flag("http", new InetSocketAddress(48080), "HTTP bind address")
  val esAddr = flag("es-addr", new InetSocketAddress(0), "Elasticsearch thrift address")
  val cacheAddr = flag("cache-addr", new InetSocketAddress(0), "Cache thrift address")

  def main() {
    val httpMux = new HttpMuxer().withHandler("/", new HTTPServerImpl(log, esAddr(), cacheAddr()))

    val httpServer = Http.serve(httpAddr(), httpMux)

    onExit {
      adminHttpServer.close()
      httpServer.close()
    }

    log.info("start admin:" +adminHttpServer.boundAddress)
    log.info("start http:"+ httpServer.boundAddress)
    Await.all(adminHttpServer,httpServer)
  }
}
