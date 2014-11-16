package com.github.iwag

import com.twitter.server.TwitterServer
import com.twitter.finagle.Service
import com.twitter.finagle.Http
import com.twitter.finagle.http.HttpMuxer
import com.twitter.io.Charsets._
import com.twitter.logging.Logger
import com.twitter.util.{Time, Future, Await}
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.handler.codec.http._

class HTTPServerImpl(log:Logger) extends Service[HttpRequest, HttpResponse]{

  override def apply(request: HttpRequest): Future[HttpResponse] = {
    log.info(s"Received at ${Time.now} ${request}")

    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    response.setContent(copiedBuffer("Hello,World!", Utf8))
    Future.value(response)
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
