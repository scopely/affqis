package com.scopely.affqis

import java.sql.{Date, ResultSetMetaData, Types, ResultSet}

import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonResultsTest extends Specification with Mockito {
  type ColumnInfo = (String, Int, String)
  val colsToTypes: Map[Int, ColumnInfo] = Map(
    1 -> ("imabigint", Types.BIGINT, "bigint"),
    2 -> ("imaninteger", Types.INTEGER, "int"),
    3 -> ("imadouble", Types.DOUBLE, "double"),
    4 -> ("imabool", Types.BOOLEAN, "boolean"),
    5 -> ("imadate", Types.DATE, "date"),
    6 -> ("imastring", Types.VARCHAR, "varchar")
  )

  def getMockResultSet = {
    val expectedLong = new java.lang.Long(1)
    val expectedDouble = java.math.BigDecimal.valueOf(500.393)
    val expectedDate = Date.valueOf("2015-01-01").toString
    val row = (expectedLong, 1, expectedDouble, true, expectedDate, "lol")
    val it = Seq(row, row, row).iterator
    val mockrs: ResultSet = mock[ResultSet]
    val mockrsm: ResultSetMetaData = mock[ResultSetMetaData]

    mockrsm.getColumnCount returns 6
    mockrsm.getColumnName(anyInt) answers { i =>
      colsToTypes(i.asInstanceOf[Int])._1
    }
    mockrsm.getColumnType(anyInt) answers { i =>
      colsToTypes(i.asInstanceOf[Int])._2
    }
    mockrsm.getColumnTypeName(anyInt) answers { i =>
      colsToTypes(i.asInstanceOf[Int])._3
    }

    mockrs.getMetaData returns mockrsm
    mockrs.next() answers { m =>
      if (it.hasNext) {
        it.next()
        true
      }
      else {
        false
      }
    }

    mockrs.getLong(1) returns expectedLong
    mockrs.getInt(2) returns row._2
    mockrs.getBigDecimal(3) returns row._3
    mockrs.getBoolean(4) returns row._4

    // We don't do anything special with dates, so it's also a getString.
    mockrs.getString(5) returns row._5

    mockrs.getString(6) returns row._6

    mockrs
  }

  "JsonResults should" >> {
    "return json strings of all ResultSet rows" >> {
      val rs = getMockResultSet
      val expectedRow =
        """
          |[{"value":1,"type":"bigint","name":"imabigint"},
          |{"value":1,"type":"int","name":"imaninteger"},
          |{"value":500.393,"type":"double","name":"imadouble"},
          |{"value":true,"type":"boolean","name":"imabool"},
          |{"value":"2015-01-01","type":"date","name":"imadate"},
          |{"value":"lol","type":"varchar","name":"imastring"}]
          |""".stripMargin.filterNot(Set('\n'))
      val results = JsonResults(rs)

      results must have size 3
      results must contain(be_==(expectedRow)).forall
    }
  }
}
