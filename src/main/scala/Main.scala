import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import net.liftweb.json._

case class Product(product_id: Option[String], product_type: Option[String])



trait Event {
    val receivedAt: Date
}
case class ProductViewedEvent(receivedAt: Date, product: Product) extends Event
case class AddedToCartEvent(receivedAt: Date, product: Product) extends Event
case class OtherEvent(receivedAt: Date) extends Event
case class NoEvent(receivedAt: Date) extends Event
case class UserEvent(anonymousId: String, event: Event)


object Main {

    val simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

    implicit val formats = new DefaultFormats {
        override def dateFormatter = simpleDateFormatter
    }

    def jsonToObject(json: String): UserEvent = {

        val parsedJson = parse(json)

        val anonymousId= (parsedJson \ "anonymousId").extract[Option[String]].orNull

        val receivedAt = (parsedJson \ "receivedAt").extract[Option[java.util.Date]].orNull

        val eventName = (parsedJson \ "event").extract[Option[String]]

        val productId = (parsedJson \ "properties" \ "product_id").extractOrElse[Option[String]](None)
        val productType = (parsedJson \ "properties" \ "product_type").extractOrElse[Option[String]](None)


        UserEvent(anonymousId,eventName match {
            case Some(eventName) if eventName == "product_viewed" => ProductViewedEvent(receivedAt,Product(productId,productType))
            case Some(eventName) if eventName == "product_added_to_cart" => AddedToCartEvent(receivedAt,Product(productId,productType))
            case Some(_) => OtherEvent(receivedAt)
            case None => NoEvent(receivedAt)
        })
    }

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("frequency_mapping").setMaster("local")

        val sc = new SparkContext(conf)
        val path_to_file = "data.txt"
        val data = sc.textFile(path_to_file).map(line => {
            jsonToObject(line)
        })

        println(data.take(1) mkString "\n")
    }
}
