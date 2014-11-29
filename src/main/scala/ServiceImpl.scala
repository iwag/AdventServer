package com.github.iwag

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import com.twitter.server.TwitterServer
import com.twitter.finagle.{Service, Thrift}
import com.twitter.util.{Future, Await}
import com.twitter.logging.Logger
import org.apache.thrift.protocol.TBinaryProtocol

import scala.collection.mutable

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

class SearchServerImpl(log:Logger) extends SearchService.FutureIface {

  val index = new AtomicInteger(0)

  val table = new mutable.HashMap[String, mutable.Set[Int]] with mutable.MultiMap[String, Int]

  def calcBigram(s: String): Set[String] = {
    if (s.length < 2) Set()
    else Range(0, s.length - 1).toList.map(v=>s(v).toString + s(v + 1)).toSet
  }

  override def put(value: String): Future[Int] = Future.value {
    val idx = index.incrementAndGet()
    calcBigram(value) foreach (s => table.getOrElseUpdate(s, mutable.Set[Int]()) += idx)

    log.info(s"put ${value} as ${idx}")
    idx
  }

  override def search(key: String): Future[SearchResponse] = Future.value{
    val bigramed = calcBigram(key)
    val foundIdxs = bigramed flatMap (table.get(_))
    val all = foundIdxs reduceLeft (_ & _)

    log.info(s"hit ${all} by ${key}")
    SearchResponse(all.toSeq, "OK")
  }

  override def delete(id: Int):Future[Unit] = Future.value {
    log.info(s"delete ${id}")
    table foreach { kv => if (kv._2.contains(id)) kv._2.remove(id)}
  }
}

trait ThriftServer extends TwitterServer {
  val thriftAddr = flag("bind", new InetSocketAddress(9090), "bind address")

  def start( service: Service[Array[Byte], Array[Byte]]
             ) {
    val server = Thrift.server.serve(thriftAddr(), service)

    onExit {
      adminHttpServer.close()
      server.close()
    }

    log.info("start admin:" + adminHttpServer.boundAddress)
    log.info("start thrift:" + server.boundAddress)
    Await.all(adminHttpServer, server)
  }
}

object SearchServer extends TwitterServer with ThriftServer {
  def main() = {
    val service = new SearchService.FinagledService(new SearchServerImpl(log), new TBinaryProtocol.Factory())

    start(service)
  }
}

object CacheServer extends TwitterServer with ThriftServer {
  def main() = {
    val service = new CacheService.FinagledService(new CacheServerImpl(log), new TBinaryProtocol.Factory())

    start(service)
  }
}
