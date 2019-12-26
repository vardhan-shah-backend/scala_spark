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

    def extractingPattern(sequence: List[Event]): List[List[Product]] = {

        def recursive(acc: List[List[Product]], remainingSequence: List[Event]): List[List[Product]] =
        remainingSequence match {
            case ProductViewedEvent(date, product) :: tailEventList =>
                recursive((product +: acc.head) :: acc.tail, tailEventList)
            case AddedToCartEvent(date,product) :: tailEventList =>
                recursive(Nil :: acc,tailEventList)
            case OtherEvent(date) :: tailEventList =>
                if (acc.head != Nil) recursive(Nil::acc,tailEventList)
                else recursive(acc,tailEventList)
            case NoEvent(date) :: tailEventList =>
                if (acc.head != Nil) recursive(Nil::acc,tailEventList)
                else recursive(acc,tailEventList)

            case Nil => acc
        }
        recursive(List(Nil),sequence)
    }

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("frequency_mapping").setMaster("local")

        val sc = new SparkContext(conf)
        val path_to_file = "data.txt"
        val data: RDD[UserEvent] = sc.textFile(path_to_file).map(jsonToObject)

        val groupedSortedData = data
            .map(userEvent => (userEvent.anonymousId, List(userEvent.event)))
            .reduceByKey((eventList1, eventList2) => eventList1 ++ eventList2)
            .mapValues(events => extractingPattern(events.sortBy(_.receivedAt)))
            .filter(pair => pair._2.tail != Nil)
            .mapValues(x => if (x.head == Nil) x.tail else x)
                .mapValues(_.flatten).map(x => (x._2,1)).reduceByKey(_+_)

        //        val groupByData: RDD[(String, Iterable[UserEvent])] = data.groupBy(userEvent => userEvent.anonymousId)

        //        val groupByKeyData: RDD[(String,Iterable[UserEvent])] = data.map(userEvent => (userEvent.anonymousId, userEvent))
        //        groupByData == groupByKeyData.groupByKey()
        //
        println(groupedSortedData.take(100) mkString "\n")

    }
}
