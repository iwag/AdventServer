package com.github.iwag

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import com.twitter.finagle.{ListeningServer, ServiceFactory, Service, Thrift}
import com.twitter.logging.Logger
import com.twitter.server.TwitterServer
import com.twitter.util.{Await, Future}
import org.apache.thrift.protocol.TBinaryProtocol

import scala.collection.mutable

