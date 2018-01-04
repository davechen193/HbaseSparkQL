package HbaseSparkQL

import HbaseSparkQL.FeatureHelper.{vector_addition}
import org.apache.spark.mllib.linalg.{Vector, Vectors, VectorUDT, SparseVector}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.mutable.WrappedArray
import scala.language.implicitConversions
 
package object SqlHelper{
    
    // perform zipWithIndex on DataFrame's like what you can do with RDD's
    def dfZipWithIndex(
        df: DataFrame,
        offset: Int = 1,
        colName: String = "id",
        inFront: Boolean = true
    ) : DataFrame = {
    df.sqlContext.createDataFrame(
        df.rdd.zipWithIndex.map(ln =>
            Row.fromSeq(
                (if (inFront) Seq(ln._2 + offset) else Seq())
                    ++ ln._1.toSeq ++
                (if (inFront) Seq() else Seq(ln._2 + offset))
            )
        ),
        StructType(
                (if (inFront) Array(StructField(colName,LongType,false)) else Array[StructField]()) 
                    ++ df.schema.fields ++ 
                (if (inFront) Array[StructField]() else Array(StructField(colName,LongType,false)))
            )
        ) 
    }

    class vector_mean extends UserDefinedAggregateFunction {
        private val vertical_size = 5851    // size of publisher verticals
        private val VecType = new VectorUDT()
        // Schema you get as an input
        def inputSchema = new StructType().add("vec", VecType)
        // Schema of the row which is used for aggregation
        def bufferSchema = new StructType().add("vec", VecType)
                               .add("count", IntegerType)
        // Returned type
        def dataType = VecType
        // Self-explaining 
        def deterministic = true
        // zero value
        def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer.update(0, Vectors.sparse(vertical_size, Array(), Array()))
            buffer.update(1, 0)
        }
        // Similar to seqOp in aggregate
        def update(buffer: MutableAggregationBuffer, input: Row) = {
            if(!input.isNullAt(0)) {
                val vec = input.getAs[Vector](0)
                val agg: Vector = buffer.get(0).asInstanceOf[Vector]
                val count: Int = buffer.getInt(1)
                val newAgg = vector_addition(agg, vec)
                buffer.update(0, newAgg)
                buffer.update(1, count + 1)
            }
        }
        // Similar to combOp in aggregate
        def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
            val agg2: Vector = buffer2.get(0).asInstanceOf[Vector]
            val agg1: Vector = buffer1.get(0).asInstanceOf[Vector]
            val count2: Int = buffer2.getInt(1)
            val count1: Int = buffer1.getInt(1)
            val newAgg = vector_addition(agg1, agg2)
            buffer1.update(0, newAgg)
            buffer1.update(1, count1 + count2)
        }
        // Called on exit to get return value
        def evaluate(buffer: Row): Vector = {
            val oldVec = buffer.get(0).asInstanceOf[SparseVector]
            val oldValues = oldVec.values
            var newIndices = new Array[Int](oldVec.indices.size)
            var newValues = new Array[Double](oldValues.size)
            val count = buffer.getInt(1)
            // calculate vector mean from vector sum
            Array.copy(oldVec.indices, 0, newIndices, 0, newIndices.length)
            for (i <- 0 to newValues.size - 1){
                newValues(i) = oldValues(i) / count
            }
            Vectors.sparse(vertical_size, newIndices, newValues)
        }
    }

    class vector_sum extends UserDefinedAggregateFunction {
        private val vertical_size = 5851    // size of publisher verticals
        private val VecType = new VectorUDT()
        // Schema you get as an input
        def inputSchema = new StructType().add("vec", VecType)
        // Schema of the row which is used for aggregation
        def bufferSchema = new StructType().add("vec", VecType)
        // Returned type
        def dataType = VecType
        // Self-explaining 
        def deterministic = true
        // zero value
        def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer.update(0, Vectors.sparse(vertical_size, Array(), Array()))
        }
        // Similar to seqOp in aggregate
        def update(buffer: MutableAggregationBuffer, input: Row) = {
            if(!input.isNullAt(0)) {
            val vec = input.getAs[Vector](0)
            val agg: Vector = buffer.get(0).asInstanceOf[Vector]
            val newAgg = vector_addition(agg, vec)
            buffer.update(0, newAgg)
            }
        }
        // Similar to combOp in aggregate
        def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
            val agg2: Vector = buffer2.get(0).asInstanceOf[Vector]
            val agg1: Vector = buffer1.get(0).asInstanceOf[Vector]
            val newAgg = vector_addition(agg1, agg2)
            buffer1.update(0, newAgg)
        }
        // Called on exit to get return value
        def evaluate(buffer: Row): Vector = {
            val vec = buffer.get(0).asInstanceOf[SparseVector]
            Vectors.sparse(vertical_size, vec.indices, vec.values)
        }
    }

    // sort grouped tuples with the second element.
    class sort_second_element extends UserDefinedAggregateFunction {
        // Schema you get as an input
        def tupSchema = new StructType(
            Array(
                StructField("_1", StringType,false),
                StructField("_2", DoubleType,false)
            )
        )
        def inputSchema = new StructType().add("tup", tupSchema)
        // Intermediate Schema
        def bufferSchema = new StructType().add("seq", ArrayType(tupSchema, false))
        // Returned type
        override def dataType = ArrayType(tupSchema, true)
        // Self-explaining
        def deterministic = true
        // zero value
        def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer.update(0, Seq())
        }
        // Similar to seqOp in aggregate
        def update(buffer: MutableAggregationBuffer, input: Row) = {
            if(!input.isNullAt(0)) {
                val sub_row = input.get(0).asInstanceOf[Row]
                val agg = buffer.get(0).asInstanceOf[Seq[Row]]
                val newAgg = agg :+ sub_row
                buffer.update(0, newAgg)
            }
        }
        // Similar to combOp in aggregate
        def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
            val agg2 = buffer2.get(0).asInstanceOf[Seq[Row]]
            val agg1 = buffer1.get(0).asInstanceOf[Seq[Row]]
            val newAgg = agg1 ++ agg2
            buffer1.update(0, newAgg)
        }
        // Called on exit to get return value
        def evaluate(buffer: Row): Seq[Row] = {
            val seq = buffer.get(0).asInstanceOf[Seq[Row]]
            seq.sortWith{(r1,r2) => 
                r1.getDouble(1) < r2.getDouble(1)
            }
        }
    }

    // aggregate RelationalGroupedDataset to a dataset of sets of given type type
    // input: 
    //       aggTypeString - type of values of the aggregated set (string, integer, or double)
    class AggregateToSet(aggTypeString: String) extends UserDefinedAggregateFunction {
        // Schema you get as an input
        assert( Seq("string", "integer", "double", "string array").contains(aggTypeString) )
        def aggSqlType = aggTypeString match {
            case "string" => StringType
            case "integer" => IntegerType
            case "double" => DoubleType
            case "string array" => ArrayType(StringType, false)
        }
        def inputSchema = new StructType().add("val", aggSqlType)
        // Intermediate Schema
        def bufferSchema = aggTypeString match {
                case "string array" => new StructType().add("seq", aggSqlType)
                case _ => new StructType().add("seq", ArrayType(aggSqlType, false))
            }
        // Returned type
        override def dataType = aggTypeString match {
            case "string array" => aggSqlType
            case _ => ArrayType(aggSqlType, true)
        }
        // Self-explaining
        def deterministic = true
        // zero value
        def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer.update(0, Seq())
        }
        // Similar to seqOp in aggregate
        def update(buffer: MutableAggregationBuffer, input: Row) = {
            if(!input.isNullAt(0)) {
                val elem = aggTypeString match {
                    case "string" => input.get(0).asInstanceOf[String]
                    case "integer" => input.get(0).asInstanceOf[Int]
                    case "double" => input.get(0).asInstanceOf[Double]
                    case "string array" => input.get(0).asInstanceOf[Seq[String]]
                }
                val agg = aggTypeString match {
                    case "string" => buffer.get(0).asInstanceOf[Seq[String]]
                    case "integer" => buffer.get(0).asInstanceOf[Seq[Int]]
                    case "double" => buffer.get(0).asInstanceOf[Seq[Double]]
                    case "string array" => buffer.get(0).asInstanceOf[Seq[String]]
                }
                val newAgg = aggTypeString match {
                    case "string array" => agg ++ elem.asInstanceOf[Seq[String]].toSet.toSeq
                    case _ => agg :+ elem
                }
                buffer.update(0, newAgg)
            }
        }
        // Similar to combOp in aggregate
        def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
            val agg2 = aggTypeString match {
                case "string" => buffer2.get(0).asInstanceOf[Seq[String]]
                case "integer" => buffer2.get(0).asInstanceOf[Seq[Int]]
                case "double" => buffer2.get(0).asInstanceOf[Seq[Double]]
                case "string array" => buffer2.get(0).asInstanceOf[Seq[String]]
            }
            val agg1 = aggTypeString match {
                case "string" => buffer1.get(0).asInstanceOf[Seq[String]]
                case "integer" => buffer1.get(0).asInstanceOf[Seq[Int]]
                case "double" => buffer1.get(0).asInstanceOf[Seq[Double]]
                case "string array" => buffer1.get(0).asInstanceOf[Seq[String]]
            }
            val newAgg = agg1 ++ agg2
            buffer1.update(0, newAgg)
        }
        // Called on exit to get return value
        def evaluate(buffer: Row): Seq[Any] = {
            val orgSeq = aggTypeString match {
                case "string" => buffer.get(0).asInstanceOf[Seq[String]]
                case "integer" => buffer.get(0).asInstanceOf[Seq[Int]]
                case "double" => buffer.get(0).asInstanceOf[Seq[Double]]
                case "string array" => buffer.get(0).asInstanceOf[Seq[String]]
            }
            orgSeq.toSet.toSeq
        }
    }
}
