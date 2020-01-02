import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

object FrequencyMapperService {
    val simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

    implicit val formats = new DefaultFormats {
        override def dateFormatter = simpleDateFormatter
    }

    case class Product(product_id: Option[String], product_type: Option[String], inCart: Boolean) {
        override def toString: String = {
            val str: StringBuilder = new StringBuilder("[" + product_id.getOrElse(None) + ", " + product_type.getOrElse(None))
            if (inCart)
                str.append(", Cart]")
            else
                str.append("]")
            str.toString()
        }
    }

    trait Event {
        val receivedAt: Date
    }

    case class ProductViewedEvent(receivedAt: Date, product: Product) extends Event

    case class AddedToCartEvent(receivedAt: Date, product: Product) extends Event

    case class OtherOrNoEvent(receivedAt: Date) extends Event

    case class UserEvent(anonymousId: String, event: Event)

    def apply(): FrequencyMapperService = new FrequencyMapperService()
}


class FrequencyMapperService extends Serializable with AbstractService {

    import FrequencyMapperService._

    type T = (List[Product], Int)

    private def filteredUserRecoPDP(parsedJson: JValue) = {


        val section: Option[String] = (parsedJson \ Constants.PROPERTIES \ Constants.SECTION).extractOrElse[Option[String]](None)

        section match {
            case Some(_) => true
            case None => false
        }

    }
    private def jsonToObject(parsedJson: JValue): UserEvent = {

        val anonymousId = (parsedJson \ Constants.ANONYMOUS_ID).extract[Option[String]].orNull

        val receivedAt = (parsedJson \ Constants.RECEIVED_AT).extract[Option[java.util.Date]].orNull

        val eventName = (parsedJson \ Constants.EVENT).extract[Option[String]]



        val productId = (parsedJson \ Constants.PROPERTIES \ Constants.PRODUCT_ID).extractOrElse[Option[String]](None)
        val productType = (parsedJson \ Constants.PROPERTIES \ Constants.PRODUCT_TYPE).extractOrElse[Option[String]](None)


        UserEvent(anonymousId, eventName match {
            case Some(eventName) if eventName == Constants.PRODUCT_VIEWED_EVENT => ProductViewedEvent(receivedAt, Product(productId, productType, inCart = false))
            case Some(eventName) if eventName == Constants.PRODUCT_ADDED_TO_CART_EVENT => AddedToCartEvent(receivedAt, Product(productId, productType, inCart = true))
            case _ => OtherOrNoEvent(receivedAt)
        })
    }

    private def extractingPattern(sequence: List[Event]): List[List[Product]] = {

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



    private def prodSeqFreqMapper(data: RDD[UserEvent]): RDD[(List[Product], Int)] = {
        val userGroupedEvents = data
            .map(userEvent => (userEvent.anonymousId, List(userEvent.event)))
            .reduceByKey((eventList1, eventList2) => eventList1 ++ eventList2).cache()

        val productSequences: RDD[List[Product]] = userGroupedEvents
            .mapValues(events => extractingPattern(events.sortBy(_.receivedAt)).map(_.reverse))
            .values.flatMap(x => x)


        val prodSeqFreqMap = productSequences.map(productList => (productList, 1)).reduceByKey(_ + _)
        prodSeqFreqMap
    }


    def resultToString(prodSeqMap: RDD[(List[Product], Int)]): RDD[String] = {

        prodSeqMap.map(pair => {
            (pair._1 mkString " -> ") + " : " + pair._2
        })

    }

    def calculate(data: RDD[String]): RDD[(List[Product], Int)] = {

        val userEventRDD: RDD[UserEvent] = data.map(parse).filter(filteredUserRecoPDP).map(jsonToObject)
        val mapper = prodSeqFreqMapper(userEventRDD)
//        resultToString(mapper)
        mapper
    }

}
