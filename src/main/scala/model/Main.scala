package model
import java.nio.file.{Files, Paths}

object Main extends App {
  override def main(args: Array[String]): Unit = {
    if( args.isEmpty ) {throw new IllegalArgumentException("Provide json data file path in command argument")}
    else if( !Files.exists(Paths.get(args(0))) ) {throw new IllegalArgumentException("File path provided but unreadable")}
    else new ModelSession(args(0))


    //Data Cleansing



    //
  }
}