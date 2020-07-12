package fr.esgi.training.spark.streaming

import org.apache.kafka.clients.producer._
import play.api.libs.json.{Json, OWrites, Writes}
import java.util.{Date, Properties, UUID}

import org.apache.avro.generic.GenericData.StringType
import org.apache.kafka.common.serialization.StringSerializer

import scala.annotation.tailrec
import scala.util.{Random, Try}
import scala.util.Random.{alphanumeric, _}
import java.text.SimpleDateFormat
import java.util.Date
case class Coordonnee(var longitude: Float, var latitude: Float)

case class Zone(var idRegion : Int,
                var RegionState : String,
                var StreetCode1 : String,
                var StreetCode2 : String,
                var StreetCode3 : String,
                var HouseNumber : String,
                var StreetName : String  )

case class Infraction(var code: String,
                      var violationDescription : String,
                      var idPhoto: String)


case class Vehicle( var plaque : String,
                    var PlateType : String,
                    var VehicleBodyType : String,
                    var VehicleMake : String,
                    var VehicleColor : String)

case class Message( var idDrone : Int ,
                    var zone : Zone,
                    var coordonnée : Coordonnee,
                    var currentDate : String,
                    var currentTime : String,
                    var violationType: Int,
                    var infraction: Infraction,
                    var vehicle :Vehicle
                    )



class ClassProducerHistorique {

  val  props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])


  val TOPIC = "json"

  val producer = new KafkaProducer[String, String](props)


  implicit val localisationJson: Writes[Zone] = Json.writes[Zone]
  implicit val infractionJson: Writes[Infraction] = Json.writes[Infraction]
  implicit val vehicleJson: Writes[Vehicle] = Json.writes[Vehicle]

  implicit val coordonneeJson: Writes[Coordonnee] = Json.writes[Coordonnee]
  implicit val messageJson: Writes[Message] = Json.writes[Message]

  val format = new java.text.SimpleDateFormat("MM/dd/yyyy")

  def tryToInt( s: String ): Option[Int] = Try(s.toInt).toOption

  def ajustHour(x: String,c: String): String = c match {
    case "P" => if (tryToInt(x.substring(0,4)) == None){"1200"}
    else {(x.substring(0,4).toInt+1200).toString}
    case "A" => x.substring(0,4)
    case "N" => "1200"
  }

  def IdRegion(x:String): Int = x match {
    case "NY" => nextInt(7) +1
    case _ => nextInt(2000)+100

  }
  def IdRobot(x: Int): Int = x match {
    case 1 => nextInt(7) + 11
    case 2 => nextInt(7) + 21
    case 3 => nextInt(7) + 31
    case 4 => nextInt(7) + 41
    case 5 => nextInt(7) + 51
    case 6 => nextInt(7) + 61
    case 7 => nextInt(7) + 71
    case 8 => nextInt(7) + 81
    case _ => nextInt(2000)+100
  }
  def IdPhoto(): String ={
    val id = List.fill(10) (alphanumeric(1))
   id.mkString

  }

  def RegionRobot(x: Int): List[(Int, Int)] = x match {
    case 1 => List((40, 42), (-72, -74))
    case 2 => List((42, 44), (-72, -74))
    case 3 => List((40, 42), (-74, -76))
    case 4 => List((42, 44), (-74, -76))
    case 5 => List((40, 42), (-76, -78))
    case 6 => List((42, 44), (-76, -78))
    case 7 => List((40, 42), (-78, -80))
    case 8 => List((42, 44), (-78, -80))
    case _ => List((30, 50), (-70, -125))
  }
  def localisationRobot(x: Int): (Float, Float)={
    val position = RegionRobot(x)
    if (x<9){
      val localisation = ((nextFloat() * 2 + position(1)._1),( -2 *nextFloat()  + position(1)._1) )
      localisation
    }else{
      val localisation = ((nextFloat() * 20 + position(1)._1),( -55 *nextFloat()  + position(1)._1) )
      localisation
    }
  }

  def TimeViolation(x:String):String={
    x.take(2) +":"+ x.tail(1)+x.tail(2)+":00"
  }
  @tailrec
  final def produce(line: Iterator[String]) {
    //verifie si ce n'est pas la dernieres ligne
    if (line.hasNext) {

      //créé une array grace a une ligne du json
      val list = line.next().replaceAll(""",(?!(?:[^"]*"[^"]*")*[^"]*$)""","").split(",")


      val desc = if (list.size>39){list.apply(39) } else { " " }


      //affecte les bonnes valeurs
      val dateo =  list.apply(4) //transformer en format date
      val hour = list.apply(19) // transformer en heure
      val hourstr = if (hour.length > 4) hour.substring(4,5) else "N"

      val region = IdRegion(list(2))
      val drone = IdRobot(region)
      val localisation = localisationRobot(region)

      val message =        Message(
        idDrone = drone,
        zone = Zone(
          idRegion = region,
          RegionState = list.apply(2),
          StreetCode1 = list.apply(9),
          StreetCode2 = list.apply(10),
          StreetCode3 = list.apply(11),
          HouseNumber = list.apply(23).toString(),
          StreetName = list.apply(24)
        ),
        coordonnée = Coordonnee(
          longitude =  localisation._1,
          latitude = localisation._2
        ),
        currentDate =dateo,
        currentTime = TimeViolation(ajustHour(hour,hourstr)),
        vehicle = Vehicle(
          plaque = list.apply(1),
          PlateType = list.apply(3),
          VehicleBodyType = list.apply(6),
          VehicleMake = list.apply(7),
          VehicleColor = list.apply(33)
        ),
        violationType = 1,
        infraction = Infraction(
          code = list.apply(5),
          violationDescription = desc,
          idPhoto = IdPhoto()
        )

      )
      val jsMsg = Json.toJson(Some(message))

      val record = new ProducerRecord[String, String](TOPIC, jsMsg.toString)
      producer.send(record)
      // val record = new ProducerRecord(TOPIC, line.next())
      produce(line)
    }
    else{
      producer.close()
    }
  }
}