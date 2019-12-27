import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import net.liftweb.json._

import scala.annotation.tailrec

case class Product(product_id: Option[String], product_type: Option[String], inCart: Boolean) {
    override def toString: String =

        product_id.getOrElse(None) + "-->" + product_type.getOrElse(None) + "-->" + {
            if (inCart) "Carted"
        }
}

trait Event {
    val receivedAt: Date
}

case class ProductViewedEvent(receivedAt: Date, product: Product) extends Event

case class AddedToCartEvent(receivedAt: Date, product: Product) extends Event

case class OtherOrNoEvent(receivedAt: Date) extends Event

case class UserEvent(anonymousId: String, event: Event)


object Main {

    val simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

    implicit val formats = new DefaultFormats {
        override def dateFormatter = simpleDateFormatter
    }

    def jsonToObject(json: String): UserEvent = {

        val parsedJson = parse(json)

        val anonymousId = (parsedJson \ "anonymousId").extract[Option[String]].orNull

        val receivedAt = (parsedJson \ "receivedAt").extract[Option[java.util.Date]].orNull

        val eventName = (parsedJson \ "event").extract[Option[String]]

        val productId = (parsedJson \ "properties" \ "product_id").extractOrElse[Option[String]](None)
        val productType = (parsedJson \ "properties" \ "product_type").extractOrElse[Option[String]](None)


        UserEvent(anonymousId, eventName match {
            case Some(eventName) if eventName == "product_viewed" => ProductViewedEvent(receivedAt, Product(productId, productType, inCart = false))
            case Some(eventName) if eventName == "product_added_to_cart" => AddedToCartEvent(receivedAt, Product(productId, productType, inCart = true))
            case _ => OtherOrNoEvent(receivedAt)
        })
    }

    def extractingPattern(sequence: List[Event]): List[List[Product]] = {

        @tailrec
        def recursive(acc: List[List[Product]], remainingSequence: List[Event]): List[List[Product]] =
            remainingSequence match {
                case ProductViewedEvent(_, product) :: tailEventList =>
                    recursive((product :: acc.head) :: acc.tail, tailEventList)
                case AddedToCartEvent(_, product) :: tailEventList =>
                    recursive(Nil :: ((product :: acc.head) :: acc.tail), tailEventList)
                case OtherOrNoEvent(_) :: tailEventList =>
                    if (acc.head.nonEmpty) recursive(Nil :: acc, tailEventList)
                    else recursive(acc, tailEventList)

                case _ :: tailEventList => recursive(acc, tailEventList)
                case Nil => if (acc.head.isEmpty) acc.tail else acc

            }

        recursive(List(Nil), sequence)
    }

    def writeFile(filename: String, s: String) = {
        val file = new File(filename)
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(s)
        bw.close()
    }

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("frequency_mapping").setMaster("local")

        val sc = new SparkContext(conf)
        val path_to_file = "data.txt"
        val data: RDD[UserEvent] = sc.textFile(path_to_file).map(jsonToObject)

        val userGroupedEvents = data
            .map(userEvent => (userEvent.anonymousId, List(userEvent.event)))
            .reduceByKey((eventList1, eventList2) => eventList1 ++ eventList2).cache()

        val products = userGroupedEvents
            .mapValues(events => extractingPattern(events.sortBy(_.receivedAt)).map(_.reverse))
            .map(_._2).reduce(_ ++ _)

        val productsRDD = sc.parallelize(products)

        val prodFreqMap = productsRDD.map(productList => (productList,1)).reduceByKey(_ + _)

        writeFile("result.txt", prodFreqMap.collect() mkString "\n")
    }
}
