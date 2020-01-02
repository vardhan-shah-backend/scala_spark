import java.text.SimpleDateFormat
import java.util.Date
//
import net.liftweb.json._
//
//val jsonString = """
//         {"anonymousId":"c86b5e74-a17d-4821-bb68-c408589d52f3","channel":"server","context":{"app":{"build":2019081302,"name":"Hopscotch","namespace":"in.hopscotch.android","version":"2.9.3"},"device":{"type":"android","adTrackingEnabled":true,"advertisingId":"b1ac6645-b633-4edd-8e17-7fb35d400cd0","id":"2c23d924be4463a7","manufacturer":"OPPO","model":"CPH1861","name":"CPH1861"},"ip":"157.44.78.53","library":{"name":"analytics-android","version":"4.3.1"},"locale":"en-US","network":{"bluetooth":false,"carrier":"Jio 4G","cellular":true,"wifi":false},"os":{"name":"Android","version":"9"},"screen":{"density":3,"height":2016,"width":1080},"timezone":"Asia/Kolkata","traits":{"anonymousId":"c86b5e74-a17d-4821-bb68-c408589d52f3","deeplink":"hopscotch://products?id=80","hs_device_id":"2c23d924be4463a7","hs_site":"android","utm_campaign":"none","utm_content":"none","utm_date":"2019-09-03 03:10:06","utm_gender":"none","utm_medium":"none","utm_source":"none","utm_term":"none"},"userAgent":"Dalvik/2.1.0 (Linux; U; Android 9; CPH1861 Build/PPR1.180610.011)"},"event":"product_listing_viewed","integrations":{"Amplitude":{"session_id":"1567503483594"},"CleverTap":false},"messageId":"2324d7d3-9791-4b0e-a6f2-1bd1b7308241","originalTimestamp":"2019-09-03T09:40:06.679Z","projectId":"GQ1JbqG7Bv","properties":{"[time] day_of_month":3,"[time] day_of_week":3,"[time] hour_of_day":15,"[time] month_of_year":9,"[time] week_of_year":"201936","add_from_details":"Search_CategoryNext","feed_size":2746,"from_screen":"Search","from_section":"girls Subcategory","funnel":"Search","funnel_section":"GirlsSubcat","funnel_tile":"Hair Accessories","plp":"Hair Accessories","plp_type":"Product listing","product_listing_id":80,"product_listing_name":"Accessories","section":"GirlsSubcat","sort_order":"Popular","universal":"none"},"receivedAt":"2019-09-03T09:40:28.548Z","sentAt":"2019-09-03T09:40:28.655Z","timestamp":"2019-09-03T09:40:06.572Z","type":"track","version":2,"writeKey":"Bsb1DpnlxWO698dtnFkU7Dzt562xO6FE"}
//         """
//
//
//
val simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
//
//
implicit val formats = new DefaultFormats {
    override def dateFormatter = simpleDateFormatter
}
//
//
//trait Event {
//    val receivedAt: Date
//}
//
//
//case class ProductViewedEvent(receivedAt: Date, product: Product) extends Event
//case class AddedToCartEvent(receivedAt: Date, product: Product) extends Event
//case class OtherEvent(receivedAt: Date) extends Event
//case class NoEvent(receivedAt: Date) extends Event
//case class UserEvent(anonymousId: String, event: Event)
//
//
//
//
//def jsonToObject(json: String): UserEvent = {
//
//    val parsedJson = parse(json)
//
//    print(parsedJson)
//    val anonymousId= (parsedJson \ "anonymousId").extract[Option[String]].orNull
//
//    val receivedAt = (parsedJson \ "receivedAt").extract[Option[java.util.Date]].orNull
//
//
//
//
//
//
//    val eventName = (parsedJson \ "event").extract[Option[String]]
//    val productId = (parsedJson \ "properties" \ "product_id").extract[Option[String]]
//    println(productId)
//    val productType = (parsedJson \ "properties" \ "product_type").extract[Option[String]]
//
//    UserEvent(anonymousId,eventName match {
//        case Some(eventName) if eventName == "product_viewed" => ProductViewedEvent(receivedAt,Product(productId,productType))
//        case Some(eventName) if eventName == "product_added_to_cart" => AddedToCartEvent(receivedAt,Product(productId,productType))
//        case Some(_) => OtherEvent(receivedAt)
//        case None => NoEvent(receivedAt)
//    })
//}
//
//jsonToObject(jsonString)

//
//map



//def extractingPattern(x: List[Boolean]): List[List[Int]] = {
//
//
//    def recursive(x: List[Boolean], acc: List[List[Int]],pos: Int): List[List[Int]] = {
//
////        println(x)
////        println(acc)
////        println(pos)
//        x match {
//
//
//            case true :: xs => recursive(xs, (pos +: acc.head) :: acc.tail, pos + 1)
//            case false :: xs =>
//                if (acc.head != Nil)
//                    recursive(xs, Nil :: acc, pos + 1)
//                else recursive(xs, acc, pos + 1)
//            case Nil => acc
//        }
//
//    }
//    recursive(x,List(),0)
//}
//
//
//extractingPattern(List(true))
//extractingPattern(List(true,true,false,false,true,false,true,true,true))




val x = List(List(List(1,2),List(3,4)),List(List(5,6,2),List(3,433,4)))
//val y = List(List(5,6),List(7,8))

x.flatMap(x => x)
//val result = scala.collection.mutable.Map[List[Int],Int]()
//
//x.map(k => result(k) = result.getOrElse(k,0) + 1 )
//
//result


List[String]().map(_.toInt)
val jsonString = "{\"hello\": [\"11\",\"12\"]}"

(parse(jsonString) \ "hello").extract[List[String]]