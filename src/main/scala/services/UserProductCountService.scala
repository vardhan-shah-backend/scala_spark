package services

import constants.Constants
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object UserProductCountService {

    implicit val formats = DefaultFormats

    trait Event {
        val productIds: List[String]
    }

    case class ProductViewedEvent(productIds: List[String]) extends Event

    case class AddedToCartEvent(productIds: List[String]) extends Event

    case class ProductOrderedEvent(productIds: List[String]) extends Event

    case class PlpScrollEvent(productIds: List[String]) extends Event

    case class OtherOrNoEvent(productIds: List[String]) extends Event

    case class UserEvent(id: Option[String], event: Event)


    class Count(
                   var impressionCount: Int,
                   var productViewCount: Int,
                   var addedToCartCount: Int,
                   var productOrderCount: Int
               ) extends Serializable {
        override def toString: String = {
            s"ImpressionCount: $impressionCount ProductViewCount: $productViewCount ATCCount: $addedToCartCount ProductOrderCount: $productOrderCount"
        }
    }

    case class UserProductCount(userId: String, productId: String, count: Count)


    def apply(): UserProductCountService = new UserProductCountService()
}

class UserProductCountService extends Serializable with AbstractService {

    import UserProductCountService._

    type T = UserProductCount

    def parseJson(jsonString: String): UserEvent = {

        val parsedJson = parse(jsonString)

        val userId = (parsedJson \ Constants.USER_ID).extractOrElse[Option[String]](None)

        val eventName = (parsedJson \ Constants.EVENT).extractOrElse[Option[String]](None)
        val productId = (parsedJson \ Constants.PROPERTIES \ Constants.PRODUCT_ID).extractOrElse[String]("")


        val event = eventName match {
            case Some(Constants.PLP_SCROLLED_EVENT) =>
                val listOfProducts = (parsedJson \ Constants.PROPERTIES \ Constants.PRODUCT_ID).extractOrElse[List[String]](List())
                PlpScrollEvent(listOfProducts)

            case Some(Constants.PRODUCT_ORDERED_EVENT) =>
                if (productId == "") ProductOrderedEvent(List()) else ProductOrderedEvent(List(productId))

            case Some(Constants.PRODUCT_VIEWED_EVENT) =>
                if (productId == "") ProductViewedEvent(List()) else ProductViewedEvent(List(productId))

            case Some(Constants.PRODUCT_ADDED_TO_CART_EVENT) =>
                if (productId == "") AddedToCartEvent(List()) else AddedToCartEvent(List(productId))

            case _ =>
                OtherOrNoEvent(List())
        }
        UserEvent(userId, event)
    }


    def eventToCount(events: Iterable[Event]): Iterable[(String,Count)] = {
        val intermediateMap: mutable.HashMap[String, Count] = mutable.HashMap()

        events.foreach(event => {
            event.productIds.foreach(productId => {
                if (!intermediateMap.contains(productId)) {
                    intermediateMap(productId) = new Count(0, 0, 0, 0)
                }
            })

            event match {
                case x: ProductViewedEvent => x.productIds.foreach(productId => intermediateMap(productId).productViewCount += 1)
                case x: PlpScrollEvent => x.productIds.foreach(intermediateMap(_).impressionCount += 1)
                case x: ProductOrderedEvent => x.productIds.foreach(intermediateMap(_).productOrderCount += 1)
                case x: AddedToCartEvent => x.productIds.foreach(intermediateMap(_).addedToCartCount += 1)
                case _ =>
            }
        })
        intermediateMap
    }

    def calculate(data: RDD[String]): RDD[UserProductCount] = {
        data.map(parseJson).filter({
            case UserEvent(None, event) => false
            case UserEvent(id, event) if event.productIds.isEmpty => false
            case _ => true
        }).map(userEvent => (userEvent.id.get, userEvent.event)).groupByKey().persist()
            .mapValues(eventToCount)
            .flatMap {
                pair => {
                    pair._2.map(productCount => UserProductCount(pair._1, productCount._1, productCount._2))
                }
            }
    }


    def resultToString(rdd: RDD[T]) = {
        rdd.map(_.toString)
    }


}