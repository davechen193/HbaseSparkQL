package HbaseSparkQL

import java.io.IOException;
import scala.util.control.Breaks._
import scala.util.Try
// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
// native modules
import HbaseSparkQL.FeatureHelper.{vector_addition}
import HbaseSparkQL.SqlHelper.{AggregateToSet}
import HbaseSparkQL.utilities.Configs
// joda datetime
import org.joda.time.DateTime
import org.joda.time.Days
// hbase & hadoop imports
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier
import org.apache.hadoop.hbase.{Cell, CellUtil, TableName, HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Delete, Get, HBaseAdmin, HTable, Put, Result, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.filter.{ColumnRangeFilter, FamilyFilter, FilterList, ValueFilter, BinaryComparator, RegexStringComparator}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat 
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.fs.{Path, FileSystem}
// spark core & sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.io.IOException

package object HbaseHelper extends Serializable{
    
    // constants
    val default_batch_size = 1000
    val hadoop_core_site_path = Configs.get("HbaseHelper.hadoop_core_site_path")
    val hbase_site_path = Configs.get("HbaseHelper.config_path")
    val HQL_reserved_words = Map(
        "get" -> Array(
            "from",
            "with",
            "with_keys",
            "use_family",
            "with_qualifiers",
            "with_qualifier_filters",
            "with_value_filters"
        ),
        "delete" -> Array(
            "from",
            "with",
            "with_keys",
            "use_family",
            "with_qualifiers"
        ),
        "scan" -> Array(
            "from",
            "use_family",
            "use_qualifier_filter",
            "use_value_filter",
            "use_timestamp_filter"
        ),
        "put" -> Array(
            "into",
            "with_records"
        ),
        "append" -> Array(
            "onto",
            "with_records"
        )
    )
    val referHbaseTable_keywords = Seq("into", "onto", "from")
    val universal_sepStr = Configs.get("HbaseHelper.separator")
    // case classes
    case class HBaseHelperException(message: String = "") extends Exception 
    case class output_string(output:String)
    case class keyed_output(id:String, output:String)
        
    // set HBase configs
    @throws(classOf[Exception])
    def hbase_init(sc:SparkContext): HBaseContext ={
        // initialize HBase Context
        val conf = HBaseConfiguration.create();
        conf.addResource(new Path(hadoop_core_site_path));
        conf.addResource(new Path(hbase_site_path));
        return new HBaseContext(sc, conf);
    }
    
    // create a table in hbase.
    // input: 
    //       tableName: name of the table
    //       families: a sequence of the column families' names
    def create_table(tableName: String, families: Seq[String]) = {
        // Create Hbase Configuration.
        val conf = HBaseConfiguration.create()
        conf.addResource(new Path(hadoop_core_site_path))
        conf.addResource(new Path(hbase_site_path))
        // Instantiating HbaseAdmin class
        val admin = new HBaseAdmin(conf)
        // Instantiating table descriptor class
        val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
        // Adding column families to table descriptor
        for (fam <- families) {
            tableDescriptor.addFamily(new HColumnDescriptor(fam))
        }
        // Execute the table through admin
        admin.createTable(tableDescriptor)
        println(" Table created: " + tableName)
    }
    
    // delete a table in hbase.
    // input:
    //       tableName: name of the table
    def delete_table(tableName: String) = {
        // Instantiating configuration class
        val conf = HBaseConfiguration.create()
        conf.addResource(new Path(hadoop_core_site_path))
        conf.addResource(new Path(hbase_site_path))
        // Instantiating HBaseAdmin class
        val admin = new HBaseAdmin(conf)
        // disabling table
        admin.disableTable(tableName)
        // Deleting table
        admin.deleteTable(tableName)
        System.out.println("Table deleted: " + tableName)
    }
    
    def valueOfCell(cell: Cell): String = {
        val ByteVal = CellUtil.cloneValue(cell)
        var value : String = ""
        if (ByteVal(0) == 0)
            value = BigInt(ByteVal).toString
        else
            value = Bytes.toString(ByteVal)
        return value
    }

    // return formatted string result from a single row result
    def formatted_string_result(result: Result, sepChar: String): String = {
        // create string result
        val sep = universal_sepStr
        var string_result = ""
        try {
            val it = result.listCells().iterator()
            val b = new StringBuilder
            while (it.hasNext) {
                val cell = it.next()
                val k = Bytes.toString(CellUtil.cloneRow(cell))
                val c = Bytes.toString(CellUtil.cloneFamily(cell))
                val q = Bytes.toString(CellUtil.cloneQualifier(cell))
                val v = valueOfCell(cell)
                val t = cell.getTimestamp // timestamp
                b.append(k + sep + c + sep + q + sep + v + sep + t + sepChar)
            }
            string_result = b.toString
            string_result = string_result.substring(0,string_result.length-1)
        } catch {
            case e @ (_ : java.lang.NullPointerException |
                      _ : java.lang.ArrayIndexOutOfBoundsException)  // valueOfCell returns error.
                => string_result = "NA"
        }
        return string_result
    }
    
    // input:
    //      1. hbaseContext: hbase context
    //      2. tableName: tableName
    //      3. cache_size: size of prefetch cache size in memory
    //      4. family: specific column family to filter
    //      5. minQualifier: lower limit of the range of qualifier to filter (inclusive)
    //      6. maxQualifier: upper limit of the range of qualifier to filter (exclusive)
    // output: an RDD of string rowkeys result that correspond to the configured scan
    @throws(classOf[Exception])
    def configuredBulkScanRowKeys(
                hbaseContext:HBaseContext,
                tableName:String,
                cache_size:Int,
                family:String,
                minQualifier:String,
                maxQualifier:String
            ): RDD[String] = {
        val filterList = new FilterList();

        filterList.addFilter( new FamilyFilter(CompareOp.EQUAL, new RegexStringComparator("^" + family + "$")) )
        filterList.addFilter( new ColumnRangeFilter(Bytes.toBytes(minQualifier),true, Bytes.toBytes(maxQualifier),true) )
        val scan = new Scan()
        scan.setCaching(cache_size)
        scan.setFilter(filterList)
        val getRdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)
        val scanResult = getRdd.map(v => Bytes.toString(v._1.get()))
        return scanResult
    }
    
    // input:
    //      spark
    //      hbaseContext
    //      rowPairsDF - the dataframe containing the pairs of row keys to merge.
    //      tableName - the name of the HBase Table.
    // The function merge of the records of rows to row1
    def mergeRows(spark:SparkSession, hbaseContext:HBaseContext, rowPairsDF: Dataset[Row], tableName: String) = {
        import spark.implicits._
        val sepStr = universal_sepStr
        
        // protect against submitting same new rowkey & old rowkey.
        val rowPairsDF_filt = rowPairsDF.filter("row1 != row2")
        
        
        if ( rowPairsDF_filt.rdd.isEmpty == false ) {
            rowPairsDF_filt.select("row1").createOrReplaceTempView("row1_" + tableName)
            rowPairsDF_filt.select("row2").createOrReplaceTempView("row2_" + tableName)
            val keyed_result2 = DistHQL(
                    spark, hbaseContext, 
                    "GET FROM " + tableName + " " + 
                    "WITH row2_" + tableName + " " +
                    "WITH_KEYS row2"
                )
                .rdd.map(r => r(0).asInstanceOf[String])
                .filter(s => s != "NA") // filter out field rows that return nothing.
                .map(s => s.split(sepStr))
                .map(arr => keyed_output(arr(0), arr.slice(1,arr.length-1).mkString(sepStr)))
                .toDF
            if (keyed_result2.count > 0) { // check if row2 return any results
                val joined_result = rowPairsDF_filt.select($"row2".alias("id"), $"row1")
                    .join(keyed_result2, Seq("id"), "right")
                val trans_result = joined_result.select("row1", "output").rdd
                    .map(r => r(0).asInstanceOf[String] + sepStr + r(1).asInstanceOf[String])
                    .map(s => output_string(s))
                    .toDF
                trans_result.createOrReplaceTempView("trans_result_" + tableName)
                // write row2 to row1                    
                DistHQL(
                    spark, hbaseContext,
                    "PUT INTO " + tableName + " " + 
                    "WITH_RECORDS trans_result_" + tableName
                )
                // delete row2
                DistHQL(
                    spark, hbaseContext, 
                    "DELETE FROM " + tableName + " " +
                    "WITH row2_" + tableName + " " +
                    "WITH_KEYS row2"
                )
            }
        }
    }
    
    // input:
    //      1. hbaseContext: hbase context
    //      2. HQL_string: a query string that allows you to "GET", "DELETE", "SCAN", and "PUT" with a formulated sql like below
    // GET query: 
    // "GET FROM hbase_table WITH query_spec_table WITH_KEYS keys1"
    // "GET FROM hbase_table WITH query_spec_table WITH_KEYS keys1 USE_FAMILY family1"
    // "GET FROM hbase_table WITH query_spec_table WITH_KEYS keys1 USE_FAMILY family1 WITH_QUALIFIERS qualifiers1"
    // "GET FROM hbase_table WITH query_spec_table WITH_KEYS keys1 USE_FAMILY family1 WITH_QUALIFIER_FILTERS minQ1 maxQ1"
    // "GET FROM hbase_table WITH query_spec_table WITH_KEYS keys1 USE_FAMILY family1 WITH_VALUE_FILTERS minV1 maxV1"
    //
    // DELETE query:
    // "DELETE FROM table1 WITH_KEYS keys1"
    // "DELETE FROM hbase_table WITH query_spec_table WITH_KEYS keys1 USE_FAMILY family1"
    // "DELETE FROM hbase_table WITH query_spec_table WITH_KEYS keys1 USE_FAMILY family1 WITH_QUALIFIERS qualifier1"
    //
    // SCAN query:
    // "SCAN FROM hbase_table USE_FAMILY family1 USE_QUALIFIER_FILTER minQ1 maxQ1"
    // "SCAN FROM hbase_table USE_FAMILY family1 USE_VALUE_FILTER minV1 maxV1"
    // "SCAN FROM hbase_table USE_FAMILY family1 USE_TIMESTAMP_FILTER minV1 maxV1"
    //
    // PUT query: 
    // "PUT INTO table1 WITH_RECORDS records1" 
    // "PUT INTO table1 WITH_RECORDS records1 USE_HFILE" 
    // (record with format: "row" + sep + "columnFamily" + sep + "column" + sep + "value")
    //
    // APPEND query: 
    // "APPEND ONTO table1 WITH_RECORDS records1" 
    // (record with format: "row" + sep + "columnFamily" + sep + "column" + sep + "value")
    //
    // note:
    //      1. filter ranges are inclusive at both ends.
    //      2. GET is more useful if you want direct access given the rows & family & qualifier
    //      3. SCAN is more useful if you want to search within all rows given the family & range of qualifiers
    //      4. If no output is needed, then an empty string dataframe will be returned.
    @throws(classOf[Exception])
    def DistHQL( spark:SparkSession, hbaseContext:HBaseContext, HQL_string:String ): Dataset[Row] = {
        import spark.implicits._
        // constants
        val sep = universal_sepStr
        val pb_schema = StructType(Array(StructField("k", StringType, true)))
        val empty_string_df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], pb_schema)
        // query settings
        val query_components = HQL_string.split(" +")
        val query_components_lowercase = query_components.map(c => c.toLowerCase)
        val nComponents = query_components.size
        var batch_size = default_batch_size
        // actual querying.
        val query_action = query_components_lowercase(0)
        var query_result = empty_string_df
        // check reserved words format.
        val reserved_checklist = HQL_reserved_words(query_action)
        var reserved_checked = 0
        var reserved_format_correct = false
        breakable {
            for ( reserved_word <- reserved_checklist){
                if ( query_components_lowercase.contains(reserved_word) == true ) {
                    val reserved_index = query_components_lowercase.indexOf(reserved_word)
                    if( reserved_word.contains("filter") && reserved_index + 2 > query_components_lowercase.size - 1 ) 
                        break
                    else if (reserved_index + 1 > query_components_lowercase.size - 1 ) 
                        break
                    else 
                        "jump to next"
                }
                reserved_checked = reserved_checked + 1
            }
        }
        if (reserved_checked < reserved_checklist.size)
            throw new HBaseHelperException("Format incorrect, please recheck input HQL query.")
        // see hbase table
        var referHbaseTable_keyword = ""
        var hbaseTableName = "not yet known"
        if (query_action == "put") {
            referHbaseTable_keyword = "into"
        } else if (query_action == "append") {
            referHbaseTable_keyword = "onto"
        } else {
            referHbaseTable_keyword = "from"
        }
        if (query_components_lowercase.contains(referHbaseTable_keyword) 
            && query_components_lowercase.indexOf(referHbaseTable_keyword) < nComponents - 1){
            val refer_hbase_index = query_components_lowercase.indexOf(referHbaseTable_keyword)
            hbaseTableName = query_components(refer_hbase_index + 1)
        } else {
            throw new HBaseHelperException(
                "Should attach keyword " +
                referHbaseTable_keywords.mkString(", or ") +
                " before the name of hbase table.")
        }
        // see query table
        var referQuerySpecTable_keyword = ""
        var querySpecTableName = "not yet known"
        var querySpecTable: DataFrame = null
        if (!Seq("scan","put", "append").contains(query_action)) {
            referQuerySpecTable_keyword = "with"
            if (query_components_lowercase.contains(referQuerySpecTable_keyword) 
                && query_components_lowercase.indexOf(referQuerySpecTable_keyword) < nComponents - 1){
                val refer_query_spec_index =
                    query_components_lowercase.indexOf(referQuerySpecTable_keyword)
                querySpecTableName = query_components(refer_query_spec_index + 1)
            } else {
                throw new HBaseHelperException(
                    "Should attach keyword 'with' before the name of query table."
                )
            }
            querySpecTable = spark.sql("select * from " + querySpecTableName)
        }
        // see family
        var family_str = ""
        if (query_components_lowercase.contains("use_family") && 
            query_components_lowercase.indexOf("use_family") < nComponents - 1){
            val useFamily_index = query_components_lowercase.indexOf("use_family")
            family_str = query_components(useFamily_index + 1)
        }
        // perform action
        if (query_action == "scan") {
            // init scan
            val scan = new Scan()
            // set filters
            val filterList = new FilterList();
            // check filters
            var use_qualifier_filter = false
            var use_value_filter = false
            var use_timestamp_filter = false
            if (query_components_lowercase.contains("use_qualifier_filter")) use_qualifier_filter = true
            if (query_components_lowercase.contains("use_value_filter")) use_value_filter = true
            if (query_components_lowercase.contains("use_timestamp_filter")) use_timestamp_filter = true
            if (family_str != ""){
                filterList.addFilter( new FamilyFilter(CompareOp.EQUAL, new RegexStringComparator("^" + family_str + "$")) )
            }
            if (use_qualifier_filter){
                try {
                    val useFilter_index = query_components_lowercase.indexOf("use_qualifier_filter")
                    val min_str = query_components(useFilter_index + 1)
                    val max_str = query_components(useFilter_index + 2)
                    filterList.addFilter( new ColumnRangeFilter(Bytes.toBytes(min_str), true, Bytes.toBytes(max_str),true) )
                } catch {
                    case _: Throwable => 
                        throw new HBaseHelperException("Please provide min & max for qualifier filter")
                }
            }
            if (use_value_filter){
                try {
                    val useFilter_index = query_components_lowercase.indexOf("use_value_filter")
                    val min_str = query_components(useFilter_index + 1)
                    val max_str = query_components(useFilter_index + 2)
                    filterList.addFilter( new ValueFilter(
                        CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(min_str))
                    ))
                    filterList.addFilter( new ValueFilter(
                        CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(max_str))
                    ))
                } catch {
                    case _: Throwable => 
                        throw new HBaseHelperException("Please provide min & max for value filter")
                }
            }
            if (use_timestamp_filter){
                var min_str = ""
                var max_str = ""
                val useFilter_index = query_components_lowercase.indexOf("use_timestamp_filter")
                try {
                    min_str = query_components(useFilter_index + 1)
                    max_str = query_components(useFilter_index + 2)
                } catch {
                    case _: Throwable => 
                        throw new HBaseHelperException("Please provide min & max for timestamp filter")
                }
                try {
                    val min_ts = min_str.toLong
                    val max_ts = max_str.toLong
                    scan.setTimeRange(min_ts, max_ts)
                } catch {
                    case _: Throwable => 
                        throw new HBaseHelperException("Should give Long formatted timestamp inputs in HQL string.")
                }
            }
            scan.setCaching(batch_size)
            if (filterList.getFilters.size > 0) {
                // use filters if given
                scan.setFilter(filterList)
            }
            query_result = hbaseContext.hbaseRDD(TableName.valueOf(hbaseTableName), scan)
                .map(v => v._2)
                .map(result => formatted_string_result(result, "\n"))
                .flatMap(s => s.split("\n"))
                .map(s => output_string(s)).toDF

        } else if (query_action == "get") {
            // row input
            var row_input:Dataset[Row] = null
            var row_input_cols: Seq[String] = Seq()
            // check filters
            var with_keys = false
            var with_qualifiers = false
            var with_qualifier_filters = false
            var with_value_filters = false
            if (query_components_lowercase.contains("with_keys")) with_keys = true
            if (query_components_lowercase.contains("with_qualifiers")) with_qualifiers = true
            if (query_components_lowercase.contains("with_qualifier_filters")) with_qualifier_filters = true
            if (query_components_lowercase.contains("with_value_filters")) with_value_filters = true
            // basic format checking.
            require(
                (family_str.length >0 == with_qualifiers) ||
                (!with_qualifiers),
                    "must specify family if qualifiers are specified.")
            require(
                (with_qualifiers != with_qualifier_filters) ||
                (with_qualifiers == false && with_qualifier_filters == false),
                    "must use either only 'with_qualifiers' or 'with_qualifier_filters'")
            // see row keys
            var row_keys_name = ""
            var row_keys:Column = null
            if (with_keys){
                try {
                    val withKeys_index = query_components_lowercase.indexOf("with_keys")
                    row_keys_name = query_components(withKeys_index + 1)
                    row_input_cols = row_input_cols :+ row_keys_name
                } catch {
                    case _: Throwable => 
                        throw new HBaseHelperException("Please provide name of 'keys' dataset for specific keys")
                }
            }
            // see qualifiers
            var qualifiers_name = "" 
            var qualifiers:Column = null
            if (with_qualifiers){
                try {
                    val useFilter_index = query_components_lowercase.indexOf("with_qualifiers")
                    qualifiers_name = query_components(useFilter_index + 1)
                    row_input_cols = row_input_cols :+ qualifiers_name
                } catch {
                    case _: Throwable => 
                        throw new HBaseHelperException("Please provide name of 'qualifiers' dataset for specific keys")
                }
            }
            // see qualifier filter
            var min_qualifiers_name = ""
            var max_qualifiers_name = ""
            var min_qualifiers:Column = null
            var max_qualifiers:Column = null
            if (with_qualifier_filters){
                try {
                    val withFilter_index = query_components_lowercase.indexOf("with_qualifier_filters")
                    min_qualifiers_name = query_components(withFilter_index + 1)
                    max_qualifiers_name = query_components(withFilter_index + 2)
                    row_input_cols = row_input_cols ++ Seq(min_qualifiers_name, max_qualifiers_name)
                } catch {
                    case _: Throwable => 
                        throw new HBaseHelperException("Please provide min & max for qualifier filter")
                }
            }
            // see value filter
            var min_values_name = ""
            var max_values_name = ""
            var min_values:Column = null
            var max_values:Column = null
            if (with_value_filters){
                try {
                    val withFilter_index = query_components_lowercase.indexOf("with_value_filters")
                    min_values_name = query_components(withFilter_index + 1)
                    max_values_name = query_components(withFilter_index + 2)
                    row_input_cols = row_input_cols ++ Seq(min_values_name, max_values_name)
                } catch {
                    case _: Throwable => 
                        throw new HBaseHelperException("Please provide min & max for value filter")
                }
            }
            row_input = querySpecTable.select(row_input_cols.map(c => col(c)): _*)
            // query for result.
            query_result = hbaseContext.bulkGet[Row, String](
                TableName.valueOf(hbaseTableName),
                batch_size,
                row_input.rdd,
                row_input_row => {
                    var get: Get = null
                    try {
                        val filterList = new FilterList();
                        // add row keys
                        val row_key_str = row_input_row.getAs[String](row_keys_name)
                        get = new Get(Bytes.toBytes(row_key_str))
                        // add family
                        if (family_str.length > 0) {
                            // "GET FROM hbase_table WITH query_spec_table WITH_KEYS keys1 USE_FAMILY family1"
                            get.addFamily( Bytes.toBytes(family_str) )
                        }
                        // add qualifiers
                        if (with_qualifiers && family_str.length > 0 && qualifiers_name.length > 0){
                            // "GET FROM hbase_table WITH query_spec_table WITH_KEYS keys1 USE_FAMILY family1 WITH_QUALIFIERS qualifier1"
                            val qualifier_str = row_input_row.getAs[String](qualifiers_name)
                            get.addColumn(
                                Bytes.toBytes(family_str), 
                                Bytes.toBytes(qualifier_str)
                            )    
                        }
                        // add qualifier filters
                        if (with_qualifier_filters) {
                            // "GET FROM hbase_table WITH query_spec_table WITH_KEYS keys1 WITH_QUALIFIER_FILTERS minQ maxQ"
                            val min_qualifier_str = row_input_row.getAs[String](min_qualifiers_name)
                            val max_qualifier_str = row_input_row.getAs[String](max_qualifiers_name)
                            filterList.addFilter( 
                                new ColumnRangeFilter(
                                    Bytes.toBytes(min_qualifier_str),
                                    true,
                                    Bytes.toBytes(max_qualifier_str),true)
                            )
                        }
                        // add value filters
                        if (with_value_filters) {
                            // "GET FROM hbase_table WITH query_spec_table WITH_KEYS keys1 WITH_VALUE_FILTERS minV maxV"
                            val min_value_str = row_input_row.getAs[String](min_values_name)
                            val max_value_str = row_input_row.getAs[String](max_values_name)
                            filterList.addFilter( 
                                new ValueFilter(
                                    CompareOp.GREATER_OR_EQUAL,
                                    new BinaryComparator(Bytes.toBytes(min_value_str))
                                )
                            )
                            filterList.addFilter(
                                new ValueFilter(
                                    CompareOp.LESS_OR_EQUAL,
                                    new BinaryComparator(Bytes.toBytes(max_value_str))
                                )
                            )
                        }
                        get.setFilter(filterList)
                    } catch {
                        case _: Throwable => 
                        throw new HBaseHelperException("something wrong is with row input: " + row_input_row.toString)
                    }
                    get
                },
                (result: Result) => {
                    formatted_string_result(result, "\n")
                }
            ).flatMap(s => s.split("\n"))
             .map(s => output_string(s)).toDF
                
        } else if (query_action == "delete") {
            // row input
            var row_input:Dataset[Row] = null
            var row_input_cols: Seq[String] = Seq()
            // see if row keys are given.
            var row_keys_name = ""
            var row_keys:Column = null
            if (query_components_lowercase.contains("with_keys") && 
                query_components_lowercase.indexOf("with_keys") < nComponents - 1){
                val with_keys_index = query_components_lowercase.indexOf("with_keys")
                row_keys_name = query_components(with_keys_index + 1)
                row_input_cols = row_input_cols :+ row_keys_name
            } else {
                throw new HBaseHelperException(
                    "Should attach keyword 'with_keys' before the given keys."
                )
            }                        
            // see qualifier
            var qualifiers_name = "" 
            var qualifiers:Column = null
            if (query_components_lowercase.contains("with_qualifiers") && 
                query_components_lowercase.indexOf("with_qualifiers") < nComponents - 1){
                val withQualifiers_index = query_components_lowercase.indexOf("with_qualifiers")
                qualifiers_name = query_components(withQualifiers_index + 1)
                row_input_cols = row_input_cols :+ qualifiers_name
            }
            row_input = querySpecTable.select(row_input_cols.map(c => col(c)): _*)
            
            // delete results. 
            hbaseContext.bulkDelete[Row](
                row_input.rdd,
                TableName.valueOf(hbaseTableName),
                row_input_row => {                  
                    val row_key_str = row_input_row.getAs[String](row_keys_name)
                    var del = new Delete(Bytes.toBytes(row_key_str))
                    if (family_str.length > 0 && qualifiers_name.length > 0){
                        // "DELETE FROM hbase_table WITH query_spec_table WITH_KEYS keys1 USE_FAMILY family1 WITH_QUALIFIERS qualifier1"
                        val qualifier_str = row_input_row.getAs[String](qualifiers_name)
                        del.addColumn(
                            Bytes.toBytes(family_str), 
                            Bytes.toBytes(qualifier_str)
                        )    
                    } else if (family_str.length > 0 && qualifiers_name.length == 0 ){
                        // "DELETE FROM hbase_table WITH query_spec_table WITH_KEYS keys1 USE_FAMILY family1"
                        del.addFamily( Bytes.toBytes(family_str) )
                    } else {
                        // "DELETE FROM hbase_table WITH query_spec_table WITH_KEYS keys1"
                        "nothing"
                    }
                    del;
                },
                batch_size
            )
                
        } else if (query_action == "put") {
            var records_name = ""
            if (query_components_lowercase.contains("with_records") &&
                query_components_lowercase.indexOf("with_records") < nComponents - 1){
                val withResults_index = query_components_lowercase.indexOf("with_records")
                records_name = query_components(withResults_index + 1)
            } else {
                throw new HBaseHelperException("Should attach keyword 'with_records' before the given records.")
            }
            val records = spark.sql("select * from " + records_name).rdd
                    .map(r => r(0).asInstanceOf[String])
                    .map(s => s.split(sep))
            // check if all records contains correct formats
            // correct formats can be either of the two: 
            //     'row :: columnFamily :: columnQualifier :: value'
            val records_length = records.map(r => r.length)
            if (records_length.filter(len => len != 4).count() == 0) {
                "All given records formats are correct."
            } else {
                throw new HBaseHelperException(
                    "All given records in 'put' should be of format 'row :: columnFamily :: columnQualifier :: value'."
                )
            }
            if (query_components_lowercase.contains("use_hfile")) {
                val path_s = "hdfs://dmp1:9000/tmp/" + java.util.UUID.randomUUID.toString
                val path = new Path(path_s)
                hbaseContext.bulkLoad(records ,TableName.valueOf(hbaseTableName),
                        (r:Array[String])=>{
                            Seq((new KeyFamilyQualifier(Bytes.toBytes(r(0)), Bytes.toBytes(r(1)), Bytes.toBytes(r(2))), Bytes.toBytes(r(3)))).iterator
                        },
                        path_s);
                val conf = HBaseConfiguration.create()
                conf.addResource(new Path(Configs.get("HbaseHelper.config_path")))
                conf.setInt("hbase.client.operation.timeout", 3600000)
                conf.setInt("hbase.rpc.timeout", 3600000)
                val table = new HTable(conf, hbaseTableName)
                val load = new LoadIncrementalHFiles(conf)
                val fs = FileSystem.get(conf)
                //Bulk load Hfiles to Hbase
                var repeat = false
                do {
                    try {
                        repeat = false
                        load.doBulkLoad(path, table)
                    }
                    catch {
                        case e: IOException => {
                            repeat = true
                            e.printStackTrace()
                        }
                    }
                } while (repeat)
                table.close()
                fs.delete(path, true)
                fs.close()
            }
            else {
                hbaseContext.bulkPut[Array[String]] (
                    records,
                    TableName.valueOf(hbaseTableName),
                    (putRecord) => {
                        try{
                            val rowkey = Bytes.toBytes(putRecord(0))
                            val family = Bytes.toBytes(putRecord(1))
                            val qualifier = Bytes.toBytes(putRecord(2))
                            val value = Bytes.toBytes(putRecord(3))
                            // update
                            val put = new Put(rowkey)
                            put.addColumn(
                                family,
                                qualifier,
                                value
                            )
                        } catch {
                            case e: java.lang.ClassCastException => {
                                println(putRecord)
                                new Put(Bytes.toBytes("test"))
                            }
                        }
                    }
                )
            }
        }  else if (query_action == "append") {
            // udaf for use.
            val AggToStringSet = new AggregateToSet("string")
            spark.udf.register("agg_to_string_set", AggToStringSet)
                
            var records_name = ""
            if (query_components_lowercase.contains("with_records") &&
                query_components_lowercase.indexOf("with_records") < nComponents - 1){
                val withResults_index = query_components_lowercase.indexOf("with_records")
                records_name = query_components(withResults_index + 1)
            } else {
                throw new HBaseHelperException("Should attach keyword 'with_records' before the given records.")
            }
            val records = spark.sql("select * from " + records_name).rdd
                .map(r => r(0).asInstanceOf[String])
                .map(s => s.split(sep))
            // check if all records contains correct formats
            // correct formats can be either of the two: 
            //     'row :: columnFamily :: columnQualifier :: value'
            val records_length = records.map(r => r.length)
            if (records_length.filter(len => len != 4).count() == 0) {
                "All given records formats are correct."
            } else {
                throw new HBaseHelperException(
                    "All given records in 'append' should be of format 'row :: columnFamily :: columnQualifier :: value'."
                )
            }
            val records_df = records
                .map(arr => (arr(0), arr(1), arr(2), arr(3)))
                .toDF("rowKey","columnFamily","columnQualifier","value")
                .withColumn("oldValue", lit(""))
                .select("rowKey","columnFamily","columnQualifier","oldValue","value")
            
            // get old records.
            val old_records_df = hbaseContext.bulkGet[Row, String](
                TableName.valueOf(hbaseTableName),
                batch_size,
                records_df.rdd,
                record_row => {
                    val filterList = new FilterList();
                    // add row keys
                    val rk_str = record_row.getAs[String]("rowKey")
                    val cf_str = record_row.getAs[String]("columnFamily")
                    val cq_str = record_row.getAs[String]("columnQualifier")
                    val val_str = record_row.getAs[String]("value")
                    var get = new Get(Bytes.toBytes(rk_str))
                    get.addFamily( Bytes.toBytes(cf_str) )
                    get.addColumn(
                        Bytes.toBytes(cf_str), 
                        Bytes.toBytes(cq_str)
                    )    
                    get
                },
                (result: Result) => {
                    formatted_string_result(result, "\n")
                }
            ).flatMap(s => s.split("\n"))
                .filter(s => s != "NA")
                .map(s => s.split(sep))
                .map(arr => (arr(0), arr(1), arr(2), arr(3)))
                .toDF("rowKey","columnFamily","columnQualifier","oldValue")
                .withColumn("value", lit(""))
                .select("rowKey","columnFamily","columnQualifier","oldValue","value")
            
            // groupBy to merge two datasets(in-order) 
            // TODO: Use reduceByKey when it's implemented in the future.
            val union_records_df = records_df.union(old_records_df)
            val merged_records_df = union_records_df
                .map(r => 
                    (
                        r.getAs[String]("rowKey") + sep + r.getAs[String]("columnFamily") + sep + r.getAs[String]("columnQualifier"),
                        r.getAs[String]("oldValue"),
                        r.getAs[String]("value")
                    )
                )
                .toDF("rowColfColq", "oldValue", "value")
                .groupBy("rowColfColq")
                .agg(Map(
                    "oldValue" -> "agg_to_string_set",
                    "value" -> "agg_to_string_set"
                    )
                )
                .select(
                    $"rowColfColq",
                    $"agg_to_string_set(oldValue)".alias("oldValue"),
                    $"agg_to_string_set(value)".alias("value")
                )
                .map(r => 
                    (
                        r.getAs[String]("rowColfColq"),
                        r.getAs[Seq[String]]("oldValue").filter(e => e!=""),
                        r.getAs[Seq[String]]("value").filter(e => e!="")
                    )
                )
                .toDF("rowColfColq","oldValue","value")
                .map(r => ( r.getAs[String]("rowColfColq"), r.getAs[Seq[String]]("oldValue") ++ r.getAs[Seq[String]]("value") ))
                .map(t => ( t._1, t._2.mkString("") ))
                .toDF("rowColfColq", "mergedValue")
                .map(r => r.getAs[String]("rowColfColq") + sep + r.getAs[String]("mergedValue"))
                .map(s => s.split(sep))
            val merged_records = merged_records_df
                .map(arr => arr.mkString(sep))
            merged_records.createOrReplaceTempView("merged_records_df")
            // write merged records
            DistHQL(
                spark, hbaseContext,
                "PUT INTO " + hbaseTableName + " " + 
                "WITH_RECORDS merged_records_df"
            )
        } else {
            throw new HBaseHelperException("Action in given HQL string is not recognized.")
        }
        return query_result
    }
    
    def tableAvailable(tableName: String): Boolean = {
        // Instantiating configuration class
        val conf = HBaseConfiguration.create()
        conf.addResource(new Path(hadoop_core_site_path))
        conf.addResource(new Path(hbase_site_path))
        // Instantiating HBaseAdmin class
        val admin = new HBaseAdmin(conf)
        return admin.tableExists(tableName) && admin.isTableEnabled(tableName)
    }
}                                                                                                                                                                                                         
