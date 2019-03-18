import java.io.PrintWriter
import java.net.ServerSocket

object DataFactory {
  def main(args: Array[String]): Unit = {
    createScoketData()
  }
  def createScoketData(): Unit = {
    val client = new ServerSocket(999).accept()

    val pw = new PrintWriter(client.getOutputStream)

    while (true) {
      Thread.sleep(2000)
      pw.println("helloword")
      pw.flush()
    }
  }
}
