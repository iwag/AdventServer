package com.github.iwag

import com.twitter.server.TwitterServer
import com.twitter.finagle.Thrift
import com.twitter.util.{Future, Await}
import com.twitter.logging.Logger
import org.apache.thrift.protocol.TBinaryProtocol

class CacheServerImpl(log:Logger) extends CacheService.FutureIface {

  val table = scala.collection.mutable.HashMap[String,String]()

  override def get(key: String): Future[CacheResponse] =
    table.get(key) match {
      case None => Future(CacheResponse(None,"not found"))
      case Some(value) => Future(CacheResponse(Some(value), "OK"))
    }

  override def put(key: String, value: String): Future[Unit] = ???
}


object CacheServer extends TwitterServer {

  val service = new CacheService.FinagledService(new CacheServerImpl(log), new TBinaryProtocol.Factory())

  val server = Thrift.server.serve(":49090", service)

  def main() {

    onExit {
      adminHttpServer.close()
      server.close()
    }

    log.info("start admin:"+adminHttpServer.boundAddress)
    log.info("start thrift:"+server.boundAddress)
    Await.all(adminHttpServer, server)
  }
}
