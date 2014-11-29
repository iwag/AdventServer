package com.github.iwag

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import com.twitter.finagle.Thrift
import com.twitter.logging.Logger
import com.twitter.server.TwitterServer
import com.twitter.util.{Await, Future}
import org.apache.thrift.protocol.TBinaryProtocol

import scala.collection.mutable

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

object SearchServer extends TwitterServer {
  val thriftAddr = flag("bind", new InetSocketAddress(9090), "bind address")

  val service = new SearchService.FinagledService(new SearchServerImpl(log), new TBinaryProtocol.Factory())

  val server = Thrift.server.serve(thriftAddr(), service)

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
