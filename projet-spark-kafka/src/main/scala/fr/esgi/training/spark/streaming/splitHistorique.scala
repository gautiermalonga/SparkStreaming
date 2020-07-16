package fr.esgi.training.spark.streaming



import org.apache.spark.sql.types._
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

object splitHistorique {



  def main(args: Array[String]): Unit = {
    val sparkSession = SparkUtils.spark()
    val sc = sparkSession.sparkContext


    val dataHistorique = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load("D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\Historique.csv")


    /*
    import scala.util.Random
    val v = Random.nextInt(100)
*/


    //GZH7067
    //.load("D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\Parking_Violations_Issued_-_Fiscal_Year_2017.csv")
    //

    /*
    val matricule = dataHistorique
      .select("Plate ID","Plate Type","Vehicle Body Type", "Vehicle Make","Vehicle Color","Vehicle Year")
      .where(col("Registration State") ==="NY")
      .dropDuplicates("Plate ID")
      .coalesce(1)

    matricule.show()

  val x =  rand()

    print( x)

    val adresse = dataHistorique
      .select("Registration State","Street Code1",	"Street Code2",	"Street Code3","House Number",	"Street Name")
      .where(col("Registration State") ==="NY")
      .distinct()
      .withColumn("x",(rand() * 4 ) + 40)
      .withColumn("y",(rand() * -8 ) - 72)
      .coalesce(1)


    adresse.show

    val infraction = dataHistorique
      .select( "Violation Code","Violation Description")
      .where(col("Registration State") ==="NY")
      .distinct()
      .coalesce(1)

    matricule.show()

    val pourcentage = dataHistorique
      .select("Vehicle Body Type","Violation Code","Violation Description")
      .where(col("Registration State") ==="NY")
      .groupBy("Vehicle Body Type","Violation Code","Violation Description")
      .count()
      .coalesce(1)

    pourcentage.show()


      matricule.write
      .format("csv")
      .option("header", "true")
      .save("D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\SparkMatricule.csv")

    adresse.write
      .format("csv")
      .option("header", "true")
      .save("D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\SparkAdresse.csv")

    infraction.write
      .format("csv")
      .option("header", "true")
      .save("D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\SparkInfraction.csv")

    pourcentage.write
      .format("csv")
      .option("header", "true")
      .save("D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\SparkPourcentage.csv")

*/
  }
}