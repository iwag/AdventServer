package com.github.iwag

import org.scalatest._
import com.twitter.logging._


class SearchSpec extends FlatSpec with Matchers with BeforeAndAfter {
  val log = LoggerFactory("log")()

  var searchImpl:SearchServerImpl = _

  before {
    searchImpl = new SearchServerImpl(log)
  }

  "search" should "bigram" in {
    searchImpl.calcBigram("waiwai") should contain theSameElementsAs Set("wa", "ai", "iw")
    searchImpl.calcBigram("こんにちは") should contain theSameElementsAs Set("こん", "んに", "にち", "ちは")
    searchImpl.calcBigram("こんこんにちは") should contain theSameElementsAs Set("こん", "んこ", "んに", "にち", "ちは")
    searchImpl.calcBigram("助けて") should contain theSameElementsAs Set("助け", "けて")
    searchImpl.calcBigram("助け") should contain theSameElementsAs Set("助け")
    searchImpl.calcBigram("あ") should contain theSameElementsAs Set()
    searchImpl.calcBigram("") should contain theSameElementsAs Set()
  }

  "search" should "put" in {
    val i = searchImpl.put("わいわい").get
    searchImpl.search("わい").get.hits should contain (i)
    searchImpl.search("わいわい").get.hits should contain (i)

    val j = searchImpl.put("こんにちわい").get
    searchImpl.search("こんにちわ").get.hits should contain (j)
    searchImpl.search("わい").get.hits should contain (i)
    searchImpl.search("わい").get.hits should contain (j)

    searchImpl.delete(i)
    searchImpl.search("わい").get.hits should not contain (i)
    searchImpl.search("わい").get.hits should contain (j)

    searchImpl.delete(j)
    searchImpl.search("わい").get.hits should not contain (j)
    searchImpl.search("わい").get.hits should not contain (j)
  }
}
