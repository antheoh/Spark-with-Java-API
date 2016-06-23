package org.myorg;

import static com.clearspring.analytics.util.TopK.string;
import static com.esotericsoftware.kryo.util.Util.string;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import static javafx.scene.input.KeyCode.T;
import javafx.scene.text.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TextInputFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import static org.apache.spark.api.python.PythonRDD.hadoopRDD;
import scala.Tuple2;
import scala.tools.nsc.matching.ParallelMatching.MatchMatrix.Row;

public class WordCount {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: WordCount <input-dir> <output-dir>");
            System.exit(1);
        }

        //SparkContext.wholeTextFiles lets you read a directory containing multiple small text files, and returns each of them as (filename, content) pairs
        SparkConf sparkConf = new SparkConf().setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //JavaRDD<String> lines = sc.textFile(args[0]);  //we create RDD our fist ds,elements are lines of input

        JavaPairRDD<String, String> files = sc.wholeTextFiles(args[0]);  //this RDD contains filename,content as key,value

        // Map each line to multiple words
        
        /*JavaRDD<String> words = lines.flatMap( //passing each element of the source through a function flatMap
                new FlatMapFunction<String, String>() {      //two args,first the type of the input,second the type of the result
            public Iterable<String> call(String line) {    //iterable the type of the result,input 
                return Arrays.asList(line.split(" "));
            }
        });   //dataset->now elements are words


        JavaRDD<String> lineCounts = files.map(new Function<Tuple2<String, String>, String>() {  //Interface Function<T1,R>,pairnei san input to string(filename) string(content) kai kanei return string
            @Override
            public String call(Tuple2<String, String> fileNameContent) throws Exception {  //call(T1 v1) 
                String content = fileNameContent._2();
                int numLines = content.split("[\r\n]+").length;
                return fileNameContent._1() + ":  " + numLines;
            }
        });   //dataset->filename:number_ofLines



        JavaRDD<String> filename = files.map(new Function<Tuple2<String, String>, String>() {  //Interface Function<T1,R>,pairnei san input to string(filename) string(content) kai kanei return string
            @Override
            public String call(Tuple2<String, String> name_of_file) throws Exception {  //call(T1 v1) 
                String onoma_arxeiou = name_of_file._1();

                return onoma_arxeiou;
            }
        });   //dataset->onoma_arxeiou
        
        
        JavaPairRDD<String, Integer> ones = words.mapToPair( //mapToPair(PairFunction<T,K2,V2> f)
                new PairFunction<String, String, Integer>() //first arg type of input,key value next
        {
            public Tuple2<String, Integer> call(final String w) {                                    //call(T t)
                return new Tuple2<String, Integer>(w.concat("_GetFileName"), 1);

            }
        });  //dataset ->now elements are key-values           
        
        */
        
        JavaPairRDD <String,String> word_with_file =files.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {
            
            @Override
            public Iterable<Tuple2<String,String>> call(Tuple2<String, String> t) throws Exception {
                 List <Tuple2<String,String>> mylist= new ArrayList<>();                 
                 String onoma_arxeiou = t._1();
                 String content=t._2();
                 String [] words=content.split("[ \t\n\r]+");
                 for(int i=0;i<words.length;i++){
                     mylist.add(new Tuple2<String,String>(onoma_arxeiou,words[i]));
                 }
                 
               return mylist;
            }
            
        });
        
        /*
        JavaPairRDD<String,Integer> ones = word_with_file.mapToPair( 
                new PairFunction<Tuple2<String,String>,String,Integer>()     //first 2 args type of input,key value next
        {
            
            
            public Tuple2<String,Integer> call(final String w,final String w2) {
                                
                return new Tuple2<String, Integer>(w.concat(w2), 1);
            }          
            
            
        });  //dataset ->now elements are key-values     
        */
        
        JavaPairRDD<String, Integer> ones = word_with_file.mapToPair( //mapToPair(PairFunction<T,K2,V2> f)
                new PairFunction <Tuple2<String,String>,String,Integer>() //first arg type of input,key value next
        {
            
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, String> t) throws Exception {
                //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                return new Tuple2<String, Integer>(t._1().concat("_"+t._2()), 1);                
            }
        });  //dataset ->now elements are key-values 
        
        
       

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {

                return i1 + i2;
            }

        }
        );

        counts.saveAsTextFile(args[1]);

        sc.stop();

    }

    //METHOD WORDS_MAPPER r
    //this method create key document-term and value 1 but we need to reduce,to mapping twn leksewn
    public static final FlatMapFunction<Tuple2<String, String>, Tuple2<Tuple2<String, String>, Integer>> WORDS_MAPPER = new FlatMapFunction<Tuple2<String, String>, Tuple2<Tuple2<String, String>, Integer>>() {

        public Iterable<Tuple2<Tuple2<String, String>, Integer>> call(Tuple2<String, String> stringIntegerTuple2) throws Exception {
            return Arrays.asList(new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(stringIntegerTuple2._1(), stringIntegerTuple2._2()), 1));
        }
    };

}