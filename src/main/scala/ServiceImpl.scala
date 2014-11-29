package com.github.iwag

import com.twitter.server.TwitterServer
import com.twitter.finagle.Thrift
import com.twitter.util.{Future, Await}
import com.twitter.logging.Logger
import org.apache.thrift.protocol.TBinaryProtocol

class CacheServerImpl(log:Logger) extends CacheService.FutureIface {
  val ExpireSeconds = 10
  val table = scala.collection.mutable.HashMap[String,(String,Long)]()

  override def get(key: String): Future[CacheResponse] =
    table.get(key) match {
      case None => {
        log.info(s"not found ${key}")
        Future(CacheResponse(None,"not found"))
      }
      case Some(v) if v._2 > System.currentTimeMillis() => {
        log.info(s"expire time ${key}")
        Future(CacheResponse(None,"expired"))
      }
      case Some(value) => {
        log.info(s"found ${key}")
        Future(CacheResponse(Some(value._1), "found"))
      }
    }

  override def put(key: String, value: String): Future[Unit] = Future.value{
    log.info(s"updating ${key}:${value}")
    val ttl = System.currentTimeMillis() + ExpireSeconds * 1000
    table(key) = (value, ttl)
  }
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
