import java.io.{BufferedWriter, File, FileWriter}


import constants.Constants
import org.apache.spark.{SparkConf, SparkContext}
import services._



object Main {

    def writeFile(filename: String, s: String) = {
        val file = new File(filename)
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(s)
        bw.close()
    }

    val FILE_NAME = "data.txt"


//    val abstractService: AbstractService = UserProductCountService()

    val abstractService: AbstractService = FrequencyMapperService()

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName(Constants.APP_NAME).setMaster(Constants.MASTER)

        val sc = new SparkContext(conf)

        val data = sc.textFile(FILE_NAME)

        val result = abstractService.calculate(data)

        println(result.collect() mkString "\n")

        Thread.sleep(100000)
    }


}
