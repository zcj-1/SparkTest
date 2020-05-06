package wordcountTemplate;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author Chaojay
 * @since 2018-12-20 18:52
 */
public class WordCountJava {
    public static void main(String[] args) {

        // 获取 SparkConf 对象，并设置 setAppName 和 setMaster。本地调试用 local[*]
        SparkConf conf = new SparkConf().setAppName("WordCountJava").setMaster("local[*]");

        // 获取 JavaSparkContext 对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 通过 textFile() 方法获取一个 JavaRDD 对象
        JavaRDD<String> rdd = sc.textFile("G:\\testFile\\wordcount\\wordcount.txt");

        // 按tab键切分，返回一个iterator
        JavaRDD<String> words = rdd.flatMap(line -> Arrays.asList(line.split("\t")).iterator());

        // 返回一个二元组计数
        JavaPairRDD wordTunple = words.mapToPair(word -> new Tuple2(word, 1));

        // 通过 reduceByKey 聚合
        JavaPairRDD wordcount = wordTunple.reduceByKey((sum1, sum2) -> (Integer) sum1 + (Integer) sum2);

        // 输出结果
        wordcount.foreach(result -> System.out.println(result));
    }
}
