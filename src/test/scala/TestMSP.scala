import java.util.Properties
import scala.collection.JavaConverters._
object TestMSP extends App {

  val targetParams = Map(
    "driver" -> "org.postgresql.Driver",
    "user" -> "postgres_user",
    "password" -> "mysecretpassword",
    "url" -> "jdbc:postgresql://localhost:5432/pgdb"
  )

  val properties = new Properties
  targetParams.foreach { case (key, value) => properties.setProperty(key, value.toString) }
  println(properties)
}
