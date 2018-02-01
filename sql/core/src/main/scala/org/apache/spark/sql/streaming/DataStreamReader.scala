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

package org.apache.spark.sql.streaming

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.types.StructType

/**
 * Interface used to load a streaming `Dataset` from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.readStream` to access this.
 * 用于从外部存储系统(例如文件系统,键值存储等)加载数据流“数据集”的接口,
  * 使用`SparkSession.readStream`来访问它。
 * @since 2.0.0
 */
@InterfaceStability.Evolving
final class DataStreamReader private[sql](sparkSession: SparkSession) extends Logging {
  /**
   * Specifies the input data source format.
   * 指定输入数据源格式
   * @since 2.0.0
   */
  def format(source: String): DataStreamReader = {
    this.source = source
    this
  }

  /**
   * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data source can
   * skip the schema inference step, and thus speed up data loading.
   * 指定输入模式,某些数据源（例如JSON）可以从数据自动推断输入模式,
    * 通过在这里指定模式,底层数据源可以跳过模式推断步骤,从而加速数据加载
   * @since 2.0.0
   */
  def schema(schema: StructType): DataStreamReader = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

  /**
   * Specifies the schema by using the input DDL-formatted string. Some data sources (e.g. JSON) can
   * infer the input schema automatically from data. By specifying the schema here, the underlying
   * data source can skip the schema inference step, and thus speed up data loading.
   * 使用输入DDL格式的字符串指定模式,某些数据源（例如JSON）可以从数据自动推断输入模式,
    * 通过在这里指定模式,底层数据源可以跳过模式推断步骤,从而加速数据加载。
   * @since 2.3.0
   */
  def schema(schemaString: String): DataStreamReader = {
    this.userSpecifiedSchema = Option(StructType.fromDDL(schemaString))
    this
  }

  /**
   * Adds an input option for the underlying data source.
   * 为基础数据源添加一个输入选项
   * You can set the following option(s):
    * 您可以设置以下选项：
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
    * `timeZone`（默认会话本地时区）：设置表示时区的字符串
   * to be used to parse timestamps in the JSON/CSV datasources or partition values.
    * 用于解析JSON/CSV数据源或分区值中的时间戳</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def option(key: String, value: String): DataStreamReader = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * Adds an input option for the underlying data source.
   * 为基础数据源添加一个输入选项
   * @since 2.0.0
   */
  def option(key: String, value: Boolean): DataStreamReader = option(key, value.toString)

  /**
   * Adds an input option for the underlying data source.
   * 为基础数据源添加一个输入选项
   * @since 2.0.0
   */
  def option(key: String, value: Long): DataStreamReader = option(key, value.toString)

  /**
   * Adds an input option for the underlying data source.
   * 为基础数据源添加一个输入选项
   * @since 2.0.0
   */
  def option(key: String, value: Double): DataStreamReader = option(key, value.toString)

  /**
   * (Scala-specific) Adds input options for the underlying data source.
   * (特定于Scala)添加底层数据源的输入选项
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
   * to be used to parse timestamps in the JSON/CSV datasources or partition values.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def options(options: scala.collection.Map[String, String]): DataStreamReader = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds input options for the underlying data source.
   * 为底层数据源添加输入选项
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
   * to be used to parse timestamps in the JSON/CSV datasources or partition values.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def options(options: java.util.Map[String, String]): DataStreamReader = {
    this.options(options.asScala)
    this
  }


  /**
   * Loads input data stream in as a `DataFrame`, for data streams that don't require a path
    * 将输入数据流加载为“DataFrame”,用于不需要路径的数据流
   * (e.g. external key-value stores).
   *
   * @since 2.0.0
   */
  def load(): DataFrame = {
    if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables, you can not " +
        "read files of Hive data source directly.")
    }

    val dataSource =
      DataSource(
        sparkSession,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = extraOptions.toMap)
    Dataset.ofRows(sparkSession, StreamingRelation(dataSource))
  }

  /**
   * Loads input in as a `DataFrame`, for data streams that read from some path.
   * 加载输入为`DataFrame`,用于从某个路径读取的数据流
   * @since 2.0.0
   */
  def load(path: String): DataFrame = {
    option("path", path).load()
  }

  /**
   * Loads a JSON file stream and returns the results as a `DataFrame`.
   * 加载JSON文件流并将结果作为“DataFrame”返回
   * <a href="http://jsonlines.org/">JSON Lines</a> (newline-delimited JSON) is supported by
   * default. For JSON (one record per file), set the `multiLine` option to true.
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   * 这个函数经过一次输入来确定输入模式,如果您事先知道该模式,请使用指定模式的版本来避免额外的扫描
   * You can set the following JSON-specific options to deal with non-standard JSON files:
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * <li>`primitivesAsString` (default `false`): infers all primitive values as a string type</li>
   * <li>`prefersDecimal` (default `false`): infers all floating-point values as a decimal
   * type. If the values do not fit in decimal, then it infers them as doubles.</li>
   * <li>`allowComments` (default `false`): ignores Java/C++ style comment in JSON records</li>
   * <li>`allowUnquotedFieldNames` (default `false`): allows unquoted JSON field names</li>
   * <li>`allowSingleQuotes` (default `true`): allows single quotes in addition to double quotes
   * </li>
   * <li>`allowNumericLeadingZeros` (default `false`): allows leading zeros in numbers
   * (e.g. 00012)</li>
   * <li>`allowBackslashEscapingAnyCharacter` (default `false`): allows accepting quoting of all
   * character using backslash quoting mechanism</li>
   * <li>`allowUnquotedControlChars` (default `false`): allows JSON Strings to contain unquoted
   * control characters (ASCII characters with value less than 32, including tab and line feed
   * characters) or not.</li>
   * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
   * during parsing.
   *   <ul>
   *     <li>`PERMISSIVE` : sets other fields to `null` when it meets a corrupted record, and puts
   *     the malformed string into a field configured by `columnNameOfCorruptRecord`. To keep
   *     corrupt records, an user can set a string type field named `columnNameOfCorruptRecord`
   *     in an user-defined schema. If a schema does not have the field, it drops corrupt records
   *     during parsing. When inferring a schema, it implicitly adds a `columnNameOfCorruptRecord`
   *     field in an output schema.</li>
   *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
   *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
   *   </ul>
   * </li>
   * <li>`columnNameOfCorruptRecord` (default is the value specified in
   * `spark.sql.columnNameOfCorruptRecord`): allows renaming the new field having malformed string
   * created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.</li>
   * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
   * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
   * date type.</li>
   * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`): sets the string that
   * indicates a timestamp format. Custom date formats follow the formats at
   * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
   * <li>`multiLine` (default `false`): parse one record, which may span multiple lines,
   * per file</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def json(path: String): DataFrame = format("json").load(path)

  /**
   * Loads a CSV file stream and returns the result as a `DataFrame`.
   * 加载CSV文件流并将结果作为“DataFrame”返回
   * This function will go through the input once to determine the input schema if `inferSchema`
   * is enabled. To avoid going through the entire data once, disable `inferSchema` option or
   * specify the schema explicitly using `schema`.
   * 如果启用“inferSchema”,则此函数将通过一次输入来确定输入模式,为了避免遍历整个数据,
    * 禁用`inferSchema`选项或者使用`schema`明确指定模式
   * You can set the following CSV-specific options to deal with CSV files:
    * 您可以设置以下CSV特定的选项来处理CSV文件：
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * <li>`sep` (default `,`): sets the single character as a separator for each
   * field and value.</li>
   * <li>`encoding` (default `UTF-8`): decodes the CSV files by the given encoding
   * type.</li>
   * <li>`quote` (default `"`): sets the single character used for escaping quoted values where
   * the separator can be part of the value. If you would like to turn off quotations, you need to
   * set not `null` but an empty string. This behaviour is different form
   * `com.databricks.spark.csv`.</li>
   * <li>`escape` (default `\`): sets the single character used for escaping quotes inside
   * an already quoted value.</li>
   * <li>`comment` (default empty string): sets the single character used for skipping lines
   * beginning with this character. By default, it is disabled.</li>
   * <li>`header` (default `false`): uses the first line as names of columns.</li>
   * <li>`inferSchema` (default `false`): infers the input schema automatically from data. It
   * requires one extra pass over the data.</li>
   * <li>`ignoreLeadingWhiteSpace` (default `false`): a flag indicating whether or not leading
   * whitespaces from values being read should be skipped.</li>
   * <li>`ignoreTrailingWhiteSpace` (default `false`): a flag indicating whether or not trailing
   * whitespaces from values being read should be skipped.</li>
   * <li>`nullValue` (default empty string): sets the string representation of a null value. Since
   * 2.0.1, this applies to all supported types including the string type.</li>
   * <li>`nanValue` (default `NaN`): sets the string representation of a non-number" value.</li>
   * <li>`positiveInf` (default `Inf`): sets the string representation of a positive infinity
   * value.</li>
   * <li>`negativeInf` (default `-Inf`): sets the string representation of a negative infinity
   * value.</li>
   * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
   * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
   * date type.</li>
   * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`): sets the string that
   * indicates a timestamp format. Custom date formats follow the formats at
   * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
   * <li>`maxColumns` (default `20480`): defines a hard limit of how many columns
   * a record can have.</li>
   * <li>`maxCharsPerColumn` (default `-1`): defines the maximum number of characters allowed
   * for any given value being read. By default, it is -1 meaning unlimited length</li>
   * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
   *    during parsing. It supports the following case-insensitive modes.
   *   <ul>
   *     <li>`PERMISSIVE` : sets other fields to `null` when it meets a corrupted record, and puts
   *     the malformed string into a field configured by `columnNameOfCorruptRecord`. To keep
   *     corrupt records, an user can set a string type field named `columnNameOfCorruptRecord`
   *     in an user-defined schema. If a schema does not have the field, it drops corrupt records
   *     during parsing. When a length of parsed CSV tokens is shorter than an expected length
   *     of a schema, it sets `null` for extra fields.</li>
   *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
   *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
   *   </ul>
   * </li>
   * <li>`columnNameOfCorruptRecord` (default is the value specified in
   * `spark.sql.columnNameOfCorruptRecord`): allows renaming the new field having malformed string
   * created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.</li>
   * <li>`multiLine` (default `false`): parse one record, which may span multiple lines.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def csv(path: String): DataFrame = format("csv").load(path)

  /**
   * Loads a Parquet file stream, returning the result as a `DataFrame`.
   * 加载Parquet文件流,将结果作为“DataFrame”返回
   * You can set the following Parquet-specific option(s) for reading Parquet files:
    * 您可以设置以下Parquet特定的选项以读取Parquet文件：
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * <li>`mergeSchema` (default is the value specified in `spark.sql.parquet.mergeSchema`): sets
   * whether we should merge schemas collected from all
   * Parquet part-files. This will override
   * `spark.sql.parquet.mergeSchema`.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def parquet(path: String): DataFrame = {
    format("parquet").load(path)
  }

  /**
   * Loads text files and returns a `DataFrame` whose schema starts with a string column named
   * "value", and followed by partitioned columns if there are any.
    * 加载文本文件并返回一个“DataFrame”,该DataFrame的模式以名为“value”的字符串列开始,如果有的话,后跟分区列。
   *
   * Each line in the text files is a new row in the resulting DataFrame. For example:
    * 文本文件中的每一行都是生成的DataFrame中的新行,例如：
   * {{{
   *   // Scala:
   *   spark.readStream.text("/path/to/directory/")
   *
   *   // Java:
   *   spark.readStream().text("/path/to/directory/")
   * }}}
   *
   * You can set the following text-specific options to deal with text files:
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def text(path: String): DataFrame = format("text").load(path)

  /**
   * Loads text file(s) and returns a `Dataset` of String. The underlying schema of the Dataset
   * contains a single string column named "value".
   * 加载文本文件并返回String的“数据集”,数据集的基础模式包含名为“value”的单个字符串列
   * If the directory structure of the text files contains partitioning information, those are
   * ignored in the resulting Dataset. To include partitioning information as columns, use `text`.
   * 如果文本文件的目录结构包含分区信息,那么在生成的数据集中将忽略这些信息,要将分区信息包含为列,请使用“text”。
   * Each line in the text file is a new element in the resulting Dataset. For example:
    * 文本文件中的每一行都是生成的数据集中的一个新元素,例如：
   * {{{
   *   // Scala:
   *   spark.readStream.textFile("/path/to/spark/README.md")
   *
   *   // Java:
   *   spark.readStream().textFile("/path/to/spark/README.md")
   * }}}
   *
   * You can set the following text-specific options to deal with text files:
    * 您可以设置以下文本特定的选项来处理文本文件：
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * </ul>
   *
   * @param path input path
   * @since 2.1.0
   */
  def textFile(path: String): Dataset[String] = {
    if (userSpecifiedSchema.nonEmpty) {
      throw new AnalysisException("User specified schema not supported with `textFile`")
    }
    text(path).select("value").as[String](sparkSession.implicits.newStringEncoder)
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = sparkSession.sessionState.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]
}
