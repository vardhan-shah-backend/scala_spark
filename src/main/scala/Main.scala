import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import net.liftweb.json._

import scala.annotation.tailrec


object Main {

    def writeFile(filename: String, s: String) = {
        val file = new File(filename)
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(s)
        bw.close()
    }

    val FILE_NAME = "data.txt"

    val frequencyMapperService = FrequencyMapperService()

//    val productEventCountService = ProductEventCountService()

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName(Constants.APP_NAME).setMaster(Constants.MASTER)

        val sc = new SparkContext(conf)

        val data = sc.textFile(FILE_NAME)

        val result = frequencyMapperService.calculate(data)

        println(result.collect() mkString "\n")
//        productEventCountService.resultToString(result).saveAsTextFile("user_product_count")

//        val stringProductSeqs: Array[String] = resultString.collect()
//
//        val prettyRepr = resultString.collect() mkString "\n"
//
//        writeFile("result.txt",prettyRepr)
//
//        println(prettyRepr)

    }


}
