package com.scopely.affqis

import java.util.concurrent.CountDownLatch

object Main extends App {
  JawampaRouter(Seq("hive"))
  new HiveWampDBClient()()

  // This is your life now.
  val latch = new CountDownLatch(1)
  latch.await()
}

