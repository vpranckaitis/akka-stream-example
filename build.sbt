val sprayJson = "io.spray" %% "spray-json" % "1.3.4"
val akkaHttp = "com.typesafe.akka" %% "akka-http" % "10.0.11"
val akkaHttpSpray = "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.11"
val akkaStream = "com.typesafe.akka" %% "akka-stream" % "2.5.9"
val jodaTime = "joda-time" % "joda-time" % "2.9.9"

lazy val `akka-stream-example` = (project in file("."))
  .settings(name := "akka-stream-example")
  .settings(libraryDependencies ++= Seq(sprayJson, akkaHttp, akkaHttpSpray, akkaStream, jodaTime))
