package com.github.iwag

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import com.twitter.finagle.Filter
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

class CacheFilter(cacheService: CacheService.FutureIface) extends SimpleFilter[HttpRequest, HttpResponse] {
  override def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]): Future[HttpResponse] = {
    cacheService.get(request.getUri) flatMap {
      _.hit match {
        case Some(v) => {
          val response = new DefaultHttpResponse(request.getProtocolVersion, HttpResponseStatus.OK)
          response.setContent(copiedBuffer(v, Utf8))
          Future(response)
        }
        case _ => {
          service(request) flatMap { res =>
            cacheService.put(request.getUri, res.getContent.toString(Utf8)) map { i =>
              res
            }
          }
        }
      }
    }
  }
}

class SearchFilter(log:Logger, searchService: SearchService.FutureIface) extends  Filter[HttpRequest, HttpResponse, List[Int], List[String]] {
  override def apply(request: HttpRequest, service: Service[List[Int], List[String]]): Future[HttpResponse] = {
    log.info(request.toString + " " + request.getUri)

    val decoded = java.net.URLDecoder.decode(request.getUri, "UTF-8")
    val keyword = decoded.split("/")(1)
    val flist = searchService.search(keyword) flatMap { res =>
      log.info(keyword + " " + res.toString)
      service(res.toList)
    }
    flist map { l =>
      val response = new DefaultHttpResponse(request.getProtocolVersion, HttpResponseStatus.OK)
      val str = l match {
        case List() => response.setStatus(HttpResponseStatus.NOT_FOUND); ""
        case list => list.mkString (",")
      }
      response.setContent (copiedBuffer(s"[${str}]", Utf8) )
      response
    }
  }
}

class StoreGetService(log:Logger, storeService: StoreService.FutureIface) extends Service[List[Int], List[String]] {
  override def apply(request: List[Int]): Future[List[String]] = {
    val list :List[Future[String]] = request.map (storeService.get(_))
    Future.collect(list) map (_.toList)
  }
}

class PostService(searchClient: SearchService.FutureIface, dbClient: StoreService.FutureIface) extends Service[HttpRequest, HttpResponse] {
  override def apply(request: HttpRequest): Future[HttpResponse] = {
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
}

class HTTPServerImpl(log: Logger, esAddr: InetSocketAddress, cacheAddr: InetSocketAddress, dbAddr: InetSocketAddress) extends Service[HttpRequest, HttpResponse] {
  private[this] lazy val cacheClient = Thrift.client.withProtocolFactory(new TBinaryProtocol.Factory()).newIface[CacheService.FutureIface]("localhost:49093")
  private[this] lazy val dbClient = Thrift.client.withProtocolFactory(new TBinaryProtocol.Factory()).newIface[StoreService.FutureIface]("localhost:49091")
  private[this] lazy val searchClient =
    Thrift.client.withProtocolFactory(new TBinaryProtocol.Factory()).newIface[SearchService.FutureIface]("localhost:49092")

  val cacheFilter = new CacheFilter(cacheClient)
  val searchFilter = new SearchFilter(log, searchClient)
  val dbService = new StoreGetService(log, dbClient)

  val post = new PostService(searchClient, dbClient)
  val get = cacheFilter andThen searchFilter andThen dbService

  override def apply(request: HttpRequest): Future[HttpResponse] = {
    request.getMethod match {
      case HttpMethod.GET => get(request)
      case HttpMethod.POST => post(request)
    }
  }

}

object HTTPServer extends TwitterServer {
  val httpAddr = flag("http", new InetSocketAddress(48080), "HTTP bind address")
  val esAddr = flag("es-addr", new InetSocketAddress(0), "Elasticsearch thrift address")
  val cacheAddr = flag("cache-addr", new InetSocketAddress(0), "Cache thrift address")
  val dbAddr = flag("db-addr", new InetSocketAddress(0), "Cache thrift address")

  def main() {
    val httpMux = new HttpMuxer().withHandler("/", new HTTPServerImpl(log, esAddr(), cacheAddr(), dbAddr()))

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
