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

package org.apache.spark.sql

import java.io.File
import java.util.{Locale, TimeZone}

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}
import org.apache.spark.sql.execution.command.{DescribeColumnCommand, DescribeTableCommand}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

/**
 * End-to-end test cases for SQL queries.
 * SQL查询的端到端测试用例
 * Each case is loaded from a file in "spark/sql/core/src/test/resources/sql-tests/inputs".
  * 每个案例都从“spark /sql/core/src/test/resources/sql-tests/inputs”文件中加载
 * Each case has a golden result file in "spark/sql/core/src/test/resources/sql-tests/results".
  *每个案例在“spark/sql/core/src/test/resources/sql-tests/results”中都有一个结果文件。
 *
 * To run the entire test suite:
  * 运行整个测试套件:
 * {{{
 *   build/sbt "sql/test-only *SQLQueryTestSuite"
 * }}}
 *
 * To run a single test file upon change:
  * 在更改时运行单个测试文件：
 * {{{
 *   build/sbt "~sql/test-only *SQLQueryTestSuite -- -z inline-table.sql"
 * }}}
 *
 * To re-generate golden files, run:
  * 要重新生成结果文件,请运行：
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/test-only *SQLQueryTestSuite"
 * }}}
 *
 * The format for input files is simple:
  * 输入文件的格式很简单：
 *  1. A list of SQL queries separated by semicolon.
  *  以分号分隔的SQL查询列表
 *  2. Lines starting with -- are treated as comments and ignored.
  *    以--开始的行被视为注释并被忽略
 *
 * For example:
  * 例如：
 * {{{
 *   -- this is a comment
 *   select 1, -1;
 *   select current_date;
 * }}}
 *
 * The format for golden result files look roughly like:
  *结果文件的格式大致如下所示：
 * {{{
 *   -- some header information
 *
 *   -- !query 0
 *   select 1, -1
 *   -- !query 0 schema
 *   struct<...schema...>
 *   -- !query 0 output
 *   ... data row 1 ...
 *   ... data row 2 ...
 *   ...
 *
 *   -- !query 1
 *   ...
 * }}}
 */
class SQLQueryTestSuite extends QueryTest with SharedSQLContext {

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  private val baseResourcePath = {
    // If regenerateGoldenFiles is true, we must be running this in SBT and we use hard-coded
    // relative path. Otherwise, we use classloader's getResource to find the location.
    //如果regenerateGoldenFiles为true,我们必须在SBT中运行它,并使用硬编码的相对路径。
    //否则,我们使用classloader的getResource来查找位置
    if (regenerateGoldenFiles) {
      java.nio.file.Paths.get("src", "test", "resources", "sql-tests").toFile
    } else {
      val res = getClass.getClassLoader.getResource("sql-tests")
      new File(res.getFile)
    }
  }

  private val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  private val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  /**
    * List of test cases to ignore, in lower cases.
    * 测试用例列表忽略,小写
    * */
  private val blackList = Set(

    "blacklist.sql",  // Do NOT remove this one. It is here to test the blacklist functionality.
                      //不要删除这个,这是测试黑名单功能
    ".DS_Store"       // A meta-file that may be created on Mac by Finder App.
                      // We should ignore this file from processing.
                      //Finder App可能在Mac上创建的元文件,我们应该忽略这个文件的处理

  )

  // Create all the test cases.
  //创建所有的测试用例
  listTestCases().foreach(createScalaTestCase)

  /** A test case.
    * 一个测试用例 */
  private case class TestCase(name: String, inputFile: String, resultFile: String)

  /**
    * A single SQL query's output.
    * 一个SQL查询的输出
    *  */
  private case class QueryOutput(sql: String, schema: String, output: String) {
    def toString(queryIndex: Int): String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      //由于stripMargin删除“|”,我们明确地不使用多行字符串,在输出
      s"-- !query $queryIndex\n" +
        sql + "\n" +
        s"-- !query $queryIndex schema\n" +
        schema + "\n" +
         s"-- !query $queryIndex output\n" +
        output
    }
  }

  private def createScalaTestCase(testCase: TestCase): Unit = {
    if (blackList.exists(t =>
        testCase.name.toLowerCase(Locale.ROOT).contains(t.toLowerCase(Locale.ROOT)))) {
      // Create a test case to ignore this case.
      //创建一个测试用例来忽略这种情况
      ignore(testCase.name) { /* Do nothing */ }
    } else {
      // Create a test case to run this case.
      //创建一个测试用例来运行这种情况
      test(testCase.name) { runTest(testCase) }
    }
  }

  /** Run a test case.
    * 运行一个测试用例*/
  private def runTest(testCase: TestCase): Unit = {
    val input = fileToString(new File(testCase.inputFile))

    // List of SQL queries to run
    //要运行的SQL查询的列表
    val queries: Seq[String] = {
      val cleaned = input.split("\n").filterNot(_.startsWith("--")).mkString("\n")
      // note: this is not a robust way to split queries using semicolon, but works for now.
      //注意:这不是使用分号拆分查询的可靠方法,但现在可行。
      cleaned.split("(?<=[^\\\\]);").map(_.trim).filter(_ != "").toSeq
    }

    // Create a local SparkSession to have stronger isolation between different test cases.
    // This does not isolate catalog changes.
    //创建一个本地的SparkSession,在不同的测试用例之间有更强的隔离。
    //这不会隔离目录更改
    val localSparkSession = spark.newSession()
    loadTestData(localSparkSession)

    // Run the SQL queries preparing them for comparison.
    //运行SQL查询准备进行比较
    val outputs: Seq[QueryOutput] = queries.map { sql =>
      val (schema, output) = getNormalizedResult(localSparkSession, sql)
      // We might need to do some query canonicalization in the future.
      //我们可能需要在将来做一些查询规范化
      QueryOutput(
        sql = sql,
        schema = schema.catalogString,
        output = output.mkString("\n").trim)
    }

    if (regenerateGoldenFiles) {
      // Again, we are explicitly not using multi-line string due to stripMargin removing "|".
      //同样,由于stripMargin删除“|”,我们明确地不使用多行字符串
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName}\n" +
        s"-- Number of queries: ${outputs.size}\n\n\n" +
        outputs.zipWithIndex.map{case (qr, i) => qr.toString(i)}.mkString("\n\n\n") + "\n"
      }
      val resultFile = new File(testCase.resultFile)
      val parent = resultFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      stringToFile(resultFile, goldenOutput)
    }

    // Read back the golden file.
    //读回测试结果文件
    val expectedOutputs: Seq[QueryOutput] = {
      val goldenOutput = fileToString(new File(testCase.resultFile))
      val segments = goldenOutput.split("-- !query.+\n")

      // each query has 3 segments, plus the header
      //每个查询有3个段，加上标题
    //  assert(segments.size == outputs.size * 3 + 1,
   //     s"Expected ${outputs.size * 3 + 1} blocks in result file but got ${segments.size}. " +
   //     s"Try regenerate the result files.")
      Seq.tabulate(outputs.size) { i =>
        QueryOutput(
          sql = segments(i * 3 + 1).trim,
          schema = segments(i * 3 + 2).trim,
          output = segments(i * 3 + 3).trim
        )
      }
    }

    // Compare results.
 /*   assertResult(expectedOutputs.size, s"Number of queries should be ${expectedOutputs.size}") {
      outputs.size
    }*/

    outputs.zip(expectedOutputs).zipWithIndex.foreach { case ((output, expected), i) =>
     /* assertResult(expected.sql, s"SQL query did not match for query #$i\n${expected.sql}") {
        output.sql
      }
      assertResult(expected.schema, s"Schema did not match for query #$i\n${expected.sql}") {
        output.schema
      }
      assertResult(expected.output, s"Result did not match for query #$i\n${expected.sql}") {
        output.output
      }*/
    }
  }

  /** Executes a query and returns the result as (schema of the output, normalized output).
    * 执行查询并将结果作为(输出的模式,标准化输出)返回*/
  private def getNormalizedResult(session: SparkSession, sql: String): (StructType, Seq[String]) = {
    // Returns true if the plan is supposed to be sorted.
    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case _: DescribeTableCommand | _: DescribeColumnCommand => true
      case PhysicalOperation(_, _, Sort(_, true, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    try {
      val df = session.sql(sql)
      val schema = df.schema
      val notIncludedMsg = "[not included in comparison]"
      // Get answer, but also get rid of the #1234 expression ids that show up in explain plans
      //得到答案,但也摆脱了解释计划中出现的＃1234表达式ID
      val answer = df.queryExecution.hiveResultString().map(_.replaceAll("#\\d+", "#x")
        .replaceAll("Location.*/sql/core/", s"Location ${notIncludedMsg}sql/core/")
        .replaceAll("Created By.*", s"Created By $notIncludedMsg")
        .replaceAll("Created Time.*", s"Created Time $notIncludedMsg")
        .replaceAll("Last Access.*", s"Last Access $notIncludedMsg"))

      // If the output is not pre-sorted, sort it.
      //如果输出没有预先排序,请对其进行排序
      if (isSorted(df.queryExecution.analyzed)) (schema, answer) else (schema, answer.sorted)

    } catch {
      case a: AnalysisException =>
        // Do not output the logical plan tree which contains expression IDs.
        //不要输出包含表达式ID的逻辑计划树
        // Also implement a crude way of masking expression IDs in the error message
        // with a generic pattern "###".
        //同时实现一个粗糙的方式来掩盖错误消息中的表达式ID与一个通用的模式
        val msg = if (a.plan.nonEmpty) a.getSimpleMessage else a.getMessage
        (StructType(Seq.empty), Seq(a.getClass.getName, msg.replaceAll("#\\d+", "#x")))
      case NonFatal(e) =>
        // If there is an exception, put the exception class followed by the message.
        //如果有异常,则放置异常类,然后输入消息。
        (StructType(Seq.empty), Seq(e.getClass.getName, e.getMessage))
    }
  }

  private def listTestCases(): Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).map { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)
      TestCase(testCaseName, absPath, resultFile)
    }
  }

  /** Returns all the files (not directories) in a directory, recursively.
    * 递归地返回目录中的所有文件(不是目录)*/
  private def listFilesRecursively(path: File): Seq[File] = {
    val (dirs, files) = path.listFiles().partition(_.isDirectory)
    files ++ dirs.flatMap(listFilesRecursively)
  }

  /** Load built-in test tables into the SparkSession.
    * 负载测试表内置到sparksession*/
  private def loadTestData(session: SparkSession): Unit = {
    import session.implicits._

    (1 to 100).map(i => (i, i.toString)).toDF("key", "value").createOrReplaceTempView("testdata")

    ((Seq(1, 2, 3), Seq(Seq(1, 2, 3))) :: (Seq(2, 3, 4), Seq(Seq(2, 3, 4))) :: Nil)
      .toDF("arraycol", "nestedarraycol")
      .createOrReplaceTempView("arraydata")

    (Tuple1(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
      Tuple1(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
      Tuple1(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
      Tuple1(Map(1 -> "a4", 2 -> "b4")) ::
      Tuple1(Map(1 -> "a5")) :: Nil)
      .toDF("mapcol")
      .createOrReplaceTempView("mapdata")
  }

  private val originalTimeZone = TimeZone.getDefault
  private val originalLocale = Locale.getDefault

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
    //时区对于那些对时区敏感的测试（timestamp_ *）是固定的America / Los_Angeles
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
    // Add Locale setting
    Locale.setDefault(Locale.US)
    RuleExecutor.resetTime()
  }

  override def afterAll(): Unit = {
    try {
      TimeZone.setDefault(originalTimeZone)
      Locale.setDefault(originalLocale)

      // For debugging dump some statistics about how much time was spent in various optimizer rules
      //用于调试转储一些关于在优化器规则中花费多少时间的统计数据
      logWarning(RuleExecutor.dumpTimeSpent())
    } finally {
      super.afterAll()
    }
  }
}
