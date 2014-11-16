package com.github.iwag

import com.twitter.server.TwitterServer
import com.twitter.util.Await

object HTTPServer extends TwitterServer {

  def main() {

    onExit {
      adminHttpServer.close()
    }

    Await.all(adminHttpServer)
  }
}
