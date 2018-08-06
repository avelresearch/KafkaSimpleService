name := "kafkasimpleservice"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-actor" % "2.5.13", "com.typesafe.akka" %% "akka-testkit" % "2.5.13" % Test)

libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-http"   % "10.1.3")

libraryDependencies += "com.typesafe.akka" %% "akka-stream-kafka" % "0.22"

libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-stream" % "2.5.13", "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.13" % Test)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"