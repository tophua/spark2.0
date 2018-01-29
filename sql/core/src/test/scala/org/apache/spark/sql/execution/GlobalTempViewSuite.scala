/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class GlobalTempViewSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    globalTempDB = spark.sharedState.globalTempViewManager.database
  }

  private var globalTempDB: String = _
  //基本的语义
  test("basic semantic") {
    try {
      sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1, 'a'")

      // If there is no database in table name, we should try local temp view first, if not found,
      //如果表名中没有数据库,我们应该先尝试本地临时视图,如果没有找到
      // try table/view in current database, which is "default" in this case. So we expect
      // NoSuchTableException here.
      //尝试当前数据库中的表/视图,在这种情况下是“默认”, 所以我们期望在这里有NoSuchTableException
      intercept[NoSuchTableException](spark.table("src"))

      // Use qualified name to refer to the global temp view explicitly.
      //使用限定的名称来显式引用全局临时视图
      checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a"))

      // Table name without database will never refer to a global temp view.
      //没有数据库的表名将永远不会引用全局临时视图
      intercept[NoSuchTableException](sql("DROP VIEW src"))

      sql(s"DROP VIEW $globalTempDB.src")
      // The global temp view should be dropped successfully.
      //全局临时视图应该成功删除
      intercept[NoSuchTableException](spark.table(s"$globalTempDB.src"))

      // We can also use Dataset API to create global temp view
      //我们也可以使用数据集API创建全局临时视图
      Seq(1 -> "a").toDF("i", "j").createGlobalTempView("src")
      checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a"))

      // Use qualified name to rename a global temp view.
      //使用限定名称来重命名全局临时视图
      sql(s"ALTER VIEW $globalTempDB.src RENAME TO src2")
      intercept[NoSuchTableException](spark.table(s"$globalTempDB.src"))
      checkAnswer(spark.table(s"$globalTempDB.src2"), Row(1, "a"))

      // Use qualified name to alter a global temp view.
      //使用限定名称来改变全局临时视图
      sql(s"ALTER VIEW $globalTempDB.src2 AS SELECT 2, 'b'")
      checkAnswer(spark.table(s"$globalTempDB.src2"), Row(2, "b"))

      // We can also use Catalog API to drop global temp view
      //我们也可以使用Catalog API来删除全局临时视图
      spark.catalog.dropGlobalTempView("src2")
      intercept[NoSuchTableException](spark.table(s"$globalTempDB.src2"))

      // We can also use Dataset API to replace global temp view
      //我们也可以使用数据集API来替换全局临时视图
      Seq(2 -> "b").toDF("i", "j").createOrReplaceGlobalTempView("src")
      checkAnswer(spark.table(s"$globalTempDB.src"), Row(2, "b"))
    } finally {
      spark.catalog.dropGlobalTempView("src")
    }
  }
  //全局临时视图在所有会话中共享
  test("global temp view is shared among all sessions") {
    try {
      sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1, 2")
      checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, 2))
      val newSession = spark.newSession()
      checkAnswer(newSession.table(s"$globalTempDB.src"), Row(1, 2))
    } finally {
      spark.catalog.dropGlobalTempView("src")
    }
  }
  //全局临时视图数据库应该被保留
  test("global temp view database should be preserved") {
    val e = intercept[AnalysisException](sql(s"CREATE DATABASE $globalTempDB"))
    assert(e.message.contains("system preserved database"))

    val e2 = intercept[AnalysisException](sql(s"USE $globalTempDB"))
    assert(e2.message.contains("system preserved database"))
  }
  //使用CREATE GLOBAL TEMP VIEW
  test("CREATE GLOBAL TEMP VIEW USING") {
    withTempPath { path =>
      try {
        Seq(1 -> "a").toDF("i", "j").write.parquet(path.getAbsolutePath)
        sql(s"CREATE GLOBAL TEMP VIEW src USING parquet OPTIONS (PATH '${path.toURI}')")
        checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a"))
        sql(s"INSERT INTO $globalTempDB.src SELECT 2, 'b'")
        checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a") :: Row(2, "b") :: Nil)
      } finally {
        spark.catalog.dropGlobalTempView("src")
      }
    }
  }
  //CREATE TABLE LIKE应该用于全局临时视图
  test("CREATE TABLE LIKE should work for global temp view") {
    try {
      sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1 AS a, '2' AS b")
      sql(s"CREATE TABLE cloned LIKE $globalTempDB.src")
      val tableMeta = spark.sessionState.catalog.getTableMetadata(TableIdentifier("cloned"))
      assert(tableMeta.schema == new StructType().add("a", "int", false).add("b", "string", false))
    } finally {
      spark.catalog.dropGlobalTempView("src")
      sql("DROP TABLE default.cloned")
    }
  }
  //列出全局临时视图
  test("list global temp views") {
    try {
      sql("CREATE GLOBAL TEMP VIEW v1 AS SELECT 3, 4")
      sql("CREATE TEMP VIEW v2 AS SELECT 1, 2")

      checkAnswer(sql(s"SHOW TABLES IN $globalTempDB"),
        Row(globalTempDB, "v1", true) ::
        Row("", "v2", true) :: Nil)

      assert(spark.catalog.listTables(globalTempDB).collect().toSeq.map(_.name) == Seq("v1", "v2"))
    } finally {
      spark.catalog.dropTempView("v1")
      spark.catalog.dropGlobalTempView("v2")
    }
  }
  //应当查找全局临时视图当且仅当全局临时数据库被指定
  test("should lookup global temp view if and only if global temp db is specified") {
    try {
      sql("CREATE GLOBAL TEMP VIEW same_name AS SELECT 3, 4")
      sql("CREATE TEMP VIEW same_name AS SELECT 1, 2")

      checkAnswer(sql("SELECT * FROM same_name"), Row(1, 2))

      // we never lookup global temp views if database is not specified in table name
      //如果没有在表名中指定数据库，我们从不查找全局临时视图
      spark.catalog.dropTempView("same_name")
      intercept[AnalysisException](sql("SELECT * FROM same_name"))

      // Use qualified name to lookup a global temp view.
      //使用限定名称来查找全局临时视图
      checkAnswer(sql(s"SELECT * FROM $globalTempDB.same_name"), Row(3, 4))
    } finally {
      spark.catalog.dropTempView("same_name")
      spark.catalog.dropGlobalTempView("same_name")
    }
  }
  //公共目录应该识别全局临时视图
  test("public Catalog should recognize global temp view") {
    try {
      sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1, 2")

      assert(spark.catalog.tableExists(globalTempDB, "src"))
      assert(spark.catalog.getTable(globalTempDB, "src").toString == new Table(
        name = "src",
        database = globalTempDB,
        description = null,
        tableType = "TEMPORARY",
        isTemporary = true).toString)
    } finally {
      spark.catalog.dropGlobalTempView("src")
    }
  }
}
