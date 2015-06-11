package com.scopely.affqis

import java.io.File

import org.apache.commons.io.FileUtils
import org.specs2.matcher.Scope
import org.specs2.mutable.After

trait WithRouterAndClient extends Scope with After {
  def dbc: WampDBClient
  def dbName: String
  val router = JawampaRouter(Seq("derby"))

  def after() = {
    println("Cleaning up router/client...")
    dbc.wampClient.close()
    router.server.stop()
    router.router.close()
    FileUtils.deleteDirectory(new File(dbName))
    new File("derby.log").delete()
  }
}
