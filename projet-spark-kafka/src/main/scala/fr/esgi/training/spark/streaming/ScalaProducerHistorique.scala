package fr.esgi.training.spark


import java.io.BufferedReader
import java.io.FileReader

import fr.esgi.training.spark.streaming.ClassProducerHistorique

import scala.annotation.tailrec
import scala.io.Source

object ScalaProducerHistorique {
  def main(args: Array[String]): Unit = {

    val listFile = List(
      "D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv",
      "D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\Parking_Violations_Issued_-_Fiscal_Year_2015.csv",
      "D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\Parking_Violations_Issued_-_Fiscal_Year_2016.csv",
      "D:\\Document D\\COURS ESGI\\SPARK\\S2\\projet\\Parking_Violations_Issued_-_Fiscal_Year_2017.csv"
    )

    def runProducer(x: String): Any = {

      val input = Source.fromFile(x).getLines()
      input.next()
      val producer = new ClassProducerHistorique
      producer.produce(input)

    }

    @tailrec
    def matchCsv(x: List[String]): Any = x match {
      case head::tail => runProducer(head);matchCsv(tail)
      case head::Nil => runProducer(head)
      case _ => println("error")
    }

    matchCsv(listFile)


  }
}