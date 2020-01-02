import net.liftweb.json.{DefaultFormats, parse}

import org.apache.spark.rdd.RDD

object ProductEventCountService {

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

    trait Count {
        val id: String
        val occurrences: Int
    }

    case class ImpressionCount(id: String, occurrences: Int) extends Count
    case class ProductViewCount(id: String, occurrences: Int) extends Count
    case class AddedToCartCount(id: String, occurrences: Int) extends Count
    case class ProductOrderCount(id: String, occurrences: Int) extends Count

    def apply(): ProductEventCountService = new ProductEventCountService()
}

class ProductEventCountService extends Serializable with AbstractService {

    import ProductEventCountService._

    type T = (String,(Iterable[ImpressionCount],Iterable[ProductViewCount],Iterable[AddedToCartCount],Iterable[ProductOrderCount]))

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
        UserEvent(userId,event)
    }



    def eventToCount(events: Iterable[Event]): (Iterable[ImpressionCount],Iterable[ProductViewCount],Iterable[AddedToCartCount],Iterable[ProductOrderCount]) = {

        val productViewCount: Iterable[ProductViewCount] = events.filter({
            case _: ProductViewedEvent => true
            case _ => false
        }).flatMap(_.productIds).groupBy(x => x).mapValues(_.size).map(pair => ProductViewCount(pair._1,pair._2))

        val productOrderCount: Iterable[ProductOrderCount] = events.filter({
            case _: ProductOrderedEvent => true
            case _ => false
        }).flatMap(_.productIds).groupBy(x => x).mapValues(_.size).map(pair => ProductOrderCount(pair._1,pair._2))

        val addedToCartCount: Iterable[AddedToCartCount] = events.filter({
            case _: AddedToCartEvent => true
            case _ => false
        }).flatMap(_.productIds).groupBy(x => x).mapValues(_.size).map(pair => AddedToCartCount(pair._1,pair._2))

        val impressionCount: Iterable[ImpressionCount] = events.filter({
            case _: PlpScrollEvent => true
            case _ => false
        }).flatMap(_.productIds).groupBy(x => x).mapValues(_.size).map(pair => ImpressionCount(pair._1,pair._2))
        (impressionCount, productViewCount, addedToCartCount, productOrderCount)
    }

    def calculate(data: RDD[String]): RDD[(String,(Iterable[ImpressionCount],Iterable[ProductViewCount],Iterable[AddedToCartCount],Iterable[ProductOrderCount]))] = {

        data.map(parseJson).filter({
            case UserEvent(None, event) => false
            case UserEvent(id,event) if event.productIds.isEmpty => false
            case _ => true
        }).map(userEvent => (userEvent.id.get, userEvent.event)).groupByKey().persist()
            .mapValues(eventToCount)
    }

    def resultToString(rdd: RDD[(String,(Iterable[ImpressionCount],Iterable[ProductViewCount],Iterable[AddedToCartCount],Iterable[ProductOrderCount]))]): RDD[String] = {

        val seperator = "------------"
        rdd.map(pair => {
            pair._1  + seperator +
            pair._2._1 mkString "\n" + seperator +
            pair._2._1 mkString "\n" + seperator +
            pair._2._3 mkString "\n" + seperator +
            pair._2._4 mkString "\n"
        })
    }


}