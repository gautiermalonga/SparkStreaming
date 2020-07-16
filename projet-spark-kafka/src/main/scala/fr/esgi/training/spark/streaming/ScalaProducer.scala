import ScalaConsumer.props
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.rand
import org.apache.spark.util.random._
import java.util.{Date, Properties, UUID}
import scala.util.Random.{alphanumeric, _}
import org.apache.spark.sql.functions.{current_date, current_timestamp, date_format}


object ScalaProducer extends App {

  import java.util.Properties
  import org.apache.kafka.clients.producer._


  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  val producer = new KafkaProducer[String, String](props)

  val TOPIC = "test"

  val sparkSession = SparkUtils.spark()
  val sc = sparkSession.sparkContext

  import sparkSession.implicits._

  val dataAdresse = sparkSession.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .load("D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\SparkAdresse\\SparkAdresse.csv")

  val dataInfraction = sparkSession.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .load("D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\SparkInfraction\\SparkInfraction.csv")

  val dataMatricule = sparkSession.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .load("D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\SparkMatricule\\SparkMatricule.csv")

  val dataRatio = sparkSession.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .load("D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\SparkPourcentage\\SparkPourcentage.csv")

  def RegionRobot(x: Int): List[(Int, Int)] = x match {
    case 1 => List((40, 42), (-72, -74))
    case 2 => List((42, 44), (-72, -74))
    case 3 => List((40, 42), (-74, -76))
    case 4 => List((42, 44), (-74, -76))
    case 5 => List((40, 42), (-76, -78))
    case 6 => List((42, 44), (-76, -78))
    case 7 => List((40, 42), (-78, -80))
    case 8 => List((42, 44), (-78, -80))
    case _ => List((0, 0), (0, 0))
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
    case _ => 0
  }

  def IdInfraction(x: Int): Boolean = {
    val i = nextInt(100)
    if ((List(1, 5).contains(x) && i > 90)
      || (List(2, 3, 7).contains(x) && i > 93)
      || (x == 4 && i > 84)
      || (x == 6 && i > 96)
      || (x == 8 && i > 88)
    ) {
      return true
    } else {
      return false
    }
  }

  def IdPhoto(): String ={
    /* val id = List.fill(10) (alphanumeric(1))
    return id.mkString*/
   UUID.randomUUID().toString
}

  val date =  Seq(1).toDF("seq").select(
    current_date().as("current_date"),
    date_format(current_timestamp(),"yyyy MM dd").as("MM-dd-yyyy"),
    date_format(current_timestamp(),"HH:mm:ss").as("HH:mm:ss"))

 // while(true)
  for ( abcd <- 1 to 1000){
    val infraction = nextInt(100)
    val idRegion = nextInt(7)+1
    val idDrone = IdRobot(idRegion)
    val reg = RegionRobot(idRegion)
    val PositionRobot = dataAdresse
      .where((col("x") > reg(0)._1 && col("x") < reg(0)._2) && (col("y") < reg(1)._1 && col("y") > reg(1)._2))
      .sample(false, (0.1))
      .first()

    val date =  Seq(1).toDF("seq").select(
      current_date().as("current_date"),
      date_format(current_timestamp(),"HH:mm:ss").as("current_time"))


    val currentDate = date.first().getDate(0)
    val currentTime = date.first().getString(1)
    /*
        val currentDate = date_format(currentTimeStamp, "MM-dd-yyyy")
        val currentTime = date_format(currentTimeStamp, "HH:mm:ss")

        println(currentDate)
    */
    val x = PositionRobot.getString(6)
    val y = PositionRobot.getString(7)
    val RegionState = PositionRobot.getString(0)
    val StreetCode1 = PositionRobot.getString(1)
    val StreetCode2 = PositionRobot.getString(2)
    val StreetCode3 = PositionRobot.getString(3)
    val HouseNumber = PositionRobot.getString(4)
    val StreetName = PositionRobot.getString(5)

    if(true){
    //(IdInfraction(idRegion)) {
      val violation = nextInt(99) + 1
      val idPhoto = IdPhoto()
      if (violation == 100) {
        val record = new ProducerRecord(TOPIC, "key", s"$idRegion;$idDrone;$currentDate;$currentTime;$x;$y;$RegionState;$StreetCode1;$StreetCode2;$StreetCode3 ;" +
          s" $HouseNumber;$StreetName;1;$idPhoto;;;;;;;")
        producer.send(record)
      }
      else {
        val vehicule = dataMatricule.sample(false, (0.0001)).first()
        val plaque = vehicule.getString(0)
        val PlateType = vehicule.getString(1)
        val VehicleBodyType = vehicule.getString(2)
        val VehicleMake = vehicule.getString(3)
        val VehicleColor = vehicule.getString(4)

        val infraction = dataRatio.where(col("Vehicle Body Type") === VehicleBodyType)
          .sample(false, 0.2)
          .first()
        val violationCode = infraction.getString(1)
        val violationDescription = infraction.getString(2)

        val record = new ProducerRecord(TOPIC, "key", s"$idRegion;$idDrone;$currentDate;$currentTime;$x;$y;$RegionState;$StreetCode1;$StreetCode2;$StreetCode3;" +
          s"$HouseNumber;$StreetName;0;$idPhoto;$plaque;$PlateType;$VehicleBodyType;$VehicleMake;$VehicleColor;$violationCode;$violationDescription")
        producer.send(record)
      }
    }
    else {
      val record = new ProducerRecord(TOPIC, "key", s"$idRegion;$idDrone;$currentDate;$currentTime;$x;$y;$RegionState;$StreetCode1;$StreetCode2;$StreetCode3;" +
        s" $HouseNumber;$StreetName;2;;;;;;;;")
      producer.send(record)
    }
  }



  producer.close()





}




/*
for(i<- 1 to 50){
  val record = new ProducerRecord(TOPIC, "key", s"hello $i ; hedi gros p$i di;aymen gros z$i zi")
  producer.send(record)
}
*/