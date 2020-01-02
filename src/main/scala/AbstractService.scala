import org.apache.spark.rdd.RDD

trait AbstractService {

    type T
    def calculate(data: RDD[String]): RDD[T]
    def resultToString(rdd: RDD[T]): RDD[String]
}
