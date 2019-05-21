import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
    def main(args: Array[String]) {
        val inputFile =  "E:/pythonWp/sparkWP/wordCount/word.txt"
        val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
        val sc = new SparkContext(conf)
                val textFile = sc.textFile(inputFile)
                val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
                wordCount.foreach(println)       
    }
}