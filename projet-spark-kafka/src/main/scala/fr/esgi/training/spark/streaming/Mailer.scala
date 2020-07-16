package fr.esgi.training.spark.streaming

import java.util.Properties

import javax.mail.{Message, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}

import scala.io.Source
import javax.mail.Message.RecipientType
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{DataFrame, Row}

object Mailer {
  val host = "smtp.gmail.com"
  val port = "587"

  val address = "aymenchabchoub150@gmail.com"
  val username = "aymenchabchoub150"
  val password = "***"

  def sendMail(text:String, subject:String) = {
    val properties = new Properties()
    properties.put("mail.smtp.port", port)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")
    properties.put("mail.smtp.ssl.trust",host)

    val session = Session.getDefaultInstance(properties, null)
    val message = new MimeMessage(session)
    message.addRecipient(RecipientType.TO, new InternetAddress("aymen95170@yahoo.fr"));
    message.setSubject(subject)
    message.setContent(text, "text/html")

    val transport = session.getTransport("smtp")
    transport.connect(host, username, password)
    transport.sendMessage(message, message.getAllRecipients)
  }
  def sendMailer(x : Row ):Unit= {
    try {
      if (x.getInt(13) == 1) {
        sendMail("( robot id : "+x.get(2).toString+ ", id Region : "+ x.get(1).toString +" ) => need humain intervention","ROBOT ID : " + x.get(2).toString)
      }
    }
  }


}