// Definere et scala-projekt med sbt.
// https://docs.scala-lang.org/getting-started/sbt-track/getting-started-with-scala-and-sbt-on-the-command-line.html

// Kør med sbt run

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object WordCount {
	def main(args: Array[String]) {
		val logFile = "/Users/claus/data/spark/sample.txt"
		// https://stackoverflow.com/questions/38008330/spark-error-a-master-url-must-be-set-in-your-configuration-when-submitting-a
		val sparkConf = new SparkConf().setAppName("Spark Word Count").setMaster("local")
		val sc = new SparkContext(sparkConf)
		val file = sc.textFile(logFile)
		val counts = file.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
		counts.saveAsTextFile("/Users/claus/data/spark/output")
	}
}