package com.scopely.affqis

import java.sql.ResultSet

import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class ResultSetIteratorTest extends Specification with Mockito {

  def getMockResultSet() = {
    val it = Seq(1, 2, 3).iterator
    val mockrs: ResultSet = mock[ResultSet]
    mockrs.next() answers { m => it.hasNext }
    mockrs.getInt(anyInt) answers { i => it.next() }
    mockrs
  }

  "ResultSetIterator" >> {
    "should wrap a ResultSet" >> {
      Try(new ResultSetIterator(getMockResultSet())) must beSuccessfulTry
    }

    "should allow iteration" >> {
      val rs: ResultSetIterator = new ResultSetIterator(getMockResultSet)
      val ints = rs map { x => x.getInt(1) }

      ints must contain(exactly(1, 2, 3))
    }
  }
}
