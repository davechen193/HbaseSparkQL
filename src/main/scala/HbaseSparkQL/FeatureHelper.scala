package HbaseSparkQL

// general imports
import scala.collection.immutable.List
import scala.reflect.ClassTag
import scala.util.control._
import scala.util.Sorting

// lift json
import net.liftweb.json._
import net.liftweb.json.JsonDSL._

// math
import scala.math.pow

// imports from spark
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseVector, SparseVector}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

package object FeatureHelper{

    @throws(classOf[Exception])
    def dense_vector_addition(vec1: Vector, vec2: Vector):Vector = {        
        // throw exceptions
        if (vec1.isInstanceOf[DenseVector] == false || vec2.isInstanceOf[DenseVector] == false){
            throw new Exception("some given parameter doesn't belong to class DenseVector!");
        }
        if(vec1.size != vec2.size){
            throw new Exception("given vectors have different sizes!");
        }
        if (vec1.size == 0){
            return vec2;    
        }else if(vec2.size == 0){
            return vec1;
        }else{
            var newVec = new Array[Double](vec1.size);
            for(i <- 0 to vec1.size-1){
                newVec(i) = vec1.apply(i) + vec2.apply(i);
            }
            return Vectors.dense(newVec);
        }
    }

    @throws(classOf[Exception])
    def dense_vector_subtraction(vec1: Vector, vec2: Vector):Vector = {
        
        // throw exceptions
        if (vec1.isInstanceOf[DenseVector] == false || vec2.isInstanceOf[DenseVector] == false){
            throw new Exception("some given parameter does't belong to class DenseVector!");
        }
        if(vec1.size != vec2.size){
            throw new Exception("given vectors have different sizes!");
        }
        if (vec1.size == 0){
            return vec2;    
        }else if(vec2.size == 0){
            return vec1;
        }else{
            var newVec = new Array[Double](vec1.size);
            for(i <- 0 to vec1.size-1){
                newVec(i) = vec1.apply(i) - vec2.apply(i);
            }
            return Vectors.dense(newVec);
        }
    }

    // filter the document with all filters in filterlist
    // each filter consist of three parts following the form: <1st arg> <operator> <2nd arg>
    // input: the document, list of filters
    @throws(classOf[Exception])
    def filter_multiple(doc: JValue, filter_list: List[String]):Boolean = {
        for (filt <- filter_list){
            val filt_str = filt.values.toString();
            val filt_parse = filt_str.split("\\s+");
            
            val oper = filt_parse(1);
            val compared = filt_parse(0);
            val compare_val =if (filt_parse.size==3) filt_parse(2).values else "NA";

            val f_val = get_feature_val(doc, compared).values;
            val f = compared.split('.');
            
            var isContained = true;
            if (oper == "eq"){ 
                isContained = f_val == compare_val;
            }else if (oper == "ne"){
                isContained = f_val != compare_val;
            }else if (oper == "lt"){
                isContained = f_val.asInstanceOf[Float] < compare_val.asInstanceOf[Float];
            }else if (oper == "gt"){
                isContained = f_val.asInstanceOf[Float] > compare_val.asInstanceOf[Float];
            }else if (oper == "not_empty"){
                isContained = f_val != "NA"
            }else{
                throw new Exception("incorrect filter parameters given!");
            }
            if (isContained == false){
                return false
            }
        }
        return true;
    }

    // get the feature value of the given f_name (feature name) in the given json document
    def get_feature_val(doc: JValue, f_name: String): JValue ={
        val f_list = f_name.split('.');
        var f_val = doc;
        val Exit = new Breaks;
        var i = 0;
        
        do{
            val f = f_list(i);
            f_val = f_val \ f;
            if (f_val == JNothing || f_val == JNull){
                return JString("NA");
            }
            i = i + 1;
        }while(i < f_list.length);

        return f_val;
    }

    @throws(classOf[Exception])
    def sparse_vector_addition(vec1: Vector, vec2: Vector):Vector = {
        if (vec1.isInstanceOf[SparseVector] == false || vec2.isInstanceOf[SparseVector] == false){
            throw new Exception("some given parameter doesn't belong to class SparseVector!");
        }
        val sVec1 = vec1.asInstanceOf[SparseVector];
        val sVec2 = vec2.asInstanceOf[SparseVector];
        val size1 = sVec1.size;
        val size2 = sVec2.size;
    
        if (size1 != size2){
            throw new Exception("given vectors have different sizes!");
        }
        val indices1 = sVec1.indices;
        val indices2 = sVec2.indices;
        val values1 = sVec1.values;
        val values2 = sVec2.values;
        val pairs1 = indices1 zip values1;
        val pairs2 = indices2 zip values2;
        Sorting.quickSort(pairs1);
        Sorting.quickSort(pairs2);
    
        val newSize = size1;
        val newIndices =(indices1.toSet).union(indices2.toSet).toArray;
        val newValues = new Array[Double](newIndices.size);
        var i = 0;
        for(index <- newIndices){
          var newValue = 0.0;
          if(indices1 contains index) newValue = newValue + values1(indices1.indexOf(index));
          if(indices2 contains index) newValue = newValue + values2(indices2.indexOf(index));
          newValues(i) = newValue;
          i = i + 1;
        }
    
        return Vectors.sparse(newSize, newIndices, newValues);
    }
    
    @throws(classOf[Exception])
    def sparse_vector_subtraction(vec1: Vector, vec2: Vector):Vector = {
        if (vec1.isInstanceOf[SparseVector] == false || vec2.isInstanceOf[SparseVector] == false){
            throw new Exception("some given parameter doesn't belong to class SparseVector!");
        }
        val sVec1 = vec1.asInstanceOf[SparseVector];
        val sVec2 = vec2.asInstanceOf[SparseVector];
        val size1 = sVec1.size;
        val size2 = sVec2.size;
    
        if (size1 != size2){
            throw new Exception("given vectors have different sizes!");
        }
        val indices1 = sVec1.indices;
        val indices2 = sVec2.indices;
        val values1 = sVec1.values;
        val values2 = sVec2.values;
        val pairs1 = indices1 zip values1;
        val pairs2 = indices2 zip values2;
        Sorting.quickSort(pairs1);
        Sorting.quickSort(pairs2);
    
        val newSize = size1;
        val newIndices =(indices1.toSet).union(indices2.toSet).toArray;
        val newValues = new Array[Double](newIndices.size);
        var i = 0;
        for(index <- newIndices){
          var newValue = 0.0;
          if(indices1 contains index) newValue = newValue + values1(indices1.indexOf(index));
          if(indices2 contains index) newValue = newValue - values2(indices2.indexOf(index));
          newValues(i) = newValue;
          i = i + 1;
        }

        return Vectors.sparse(newSize, newIndices, newValues);
    }

    // addition of the two vectors
    @throws(classOf[Exception])
    def vector_addition(vec1: Vector, vec2: Vector):Vector = (vec1, vec2) match{
        case (a: SparseVector, b: SparseVector) => sparse_vector_addition(vec1,vec2)
        case (a: DenseVector, b: DenseVector) => dense_vector_addition(vec1,vec2)
        case _ => throw new Exception("Type mismatch or Non-Vector input!")
    }

    // subtraction of the two vectors
    @throws(classOf[Exception])
    def vector_subtraction(vec1: Vector, vec2: Vector):Vector = (vec1, vec2) match{
        case (a: SparseVector, b: SparseVector) => sparse_vector_subtraction(vec1,vec2)
        case (a: DenseVector, b: DenseVector) => dense_vector_subtraction(vec1,vec2)
        case _ => throw new Exception("Type mismatch or Non-Vector input!")
    }
    
    @throws(classOf[Exception])
    def sparse_vector_divide_by_constant(vec: Vector, C: Double):Vector = {
        if (vec.isInstanceOf[SparseVector] == false){
            throw new Exception("some given parameter doesn't belong to class SparseVector!")
        }
        val sVec = vec.asInstanceOf[SparseVector]
        val size = sVec.size
        val indices = sVec.indices
        val values = sVec.values
        val newValues = values.map(value => value/C)
        return Vectors.sparse(size, indices, newValues)
    }
    
    // divide a vector by a constant C
    def vector_divide_by_constant(vec: Vector, C: Double):Vector = vec match{
        case _:SparseVector => sparse_vector_divide_by_constant(vec, C)
        case _:DenseVector => new DenseVector(vec.toArray.map(value => value / C))
        case _ => throw new Exception("Type mismatch or Non-Vector input!")
    }
    
    implicit class PowerInt(i: Int) {
        def ** (b: Int): Int = pow(i, b).intValue;
    }
}                                                                                                                                                                                                          
