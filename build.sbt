import AssemblyKeys._

name := "CruncherWebUI"

version := "0.1"

scalaVersion := "2.11.2"

packSettings

seq(
        // [Optional] Specify mappings from program name -> Main class (full package path)
        packMain := Map(
        "WebServer" -> "ui.Webserver"
        ,"CruncherUI" -> "cruncher.ui.CruncherWebserver"
        ,"TornadoUI" -> "tornado.ui.TornadoWebserver"
        ,"TwitterHBCExample" -> "examples.TwitterHBCExample"
        ,"Twitter4JExample" -> "tornado.examples.TwitterStreamAPI"
        ,"TornadoStormKafkaTest" -> "examples.TornadoStormKafkaTest"
        ,"TestRandomTextGen" -> "tornado.ui.TestRandomTextGen"
        ,"StreamConroller" -> "utils.StreamController"    
        ,"KafkaFileProducer" -> "utils.KafkaFileProducer"
        ,"Playground" -> "utils.Playground"
        ,"MockupSparkApp" -> "utils.MockSparkApp"
        ,"MockSparkStreamingApp" -> "utils.MockSparkStreamingApp"        
	),
        // Add custom settings here
        // [Optional] JVM options of scripts (program name -> Seq(JVM option, ...))
        //packJvmOpts := Map("WebServer" -> Seq("-Xmx1048m")
	//),
        // [Optional] Extra class paths to look when launching a program
        packExtraClasspath := Map("main" -> Seq("${PROG_HOME}/etc")), 
        // [Optional] (Generate .bat files for Windows. The default value is true)
        packGenerateWindowsBatFile := false,
        // [Optional] jar file name format in pack/lib folder (Since 0.5.0)
        //   "default"   (project name)-(version).jar 
        //   "full"      (organization name).(project name)-(version).jar
        //   "no-version" (organization name).(project name).jar
        //   "original"  (Preserve original jar file names)
        packJarNameConvention := "default",
        // [Optional] List full class paths in the launch scripts (default is false) (since 0.5.1)
        packExpandedClasspath := true
      ) 

resolvers ++= Seq(
			"spray repo" at "http://repo.spray.io",
			"spray nightlies" at "http://nightlies.spray.io",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/",
  "Secured Central Repository" at "https://repo1.maven.org/maven2"
)

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "com.typesafe.akka"  %% "akka-actor"       % "2.3.6",
  "com.typesafe.akka"  %% "akka-slf4j"       % "2.3.6",
  "com.typesafe.akka"  %% "akka-remote"       % "2.3.6",
  "io.spray"           %% "spray-can"        % "1.3.2",
  "io.spray"           %% "spray-routing"    % "1.3.2",
  "io.spray"           %% "spray-httpx"    % "1.3.2",
  "io.spray"           %% "spray-json"       % "1.3.0",
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "com.twitter" % "hbc-core" % "2.2.0",
  "com.twitter" % "hbc-twitter4j" % "2.2.0",  
  "joda-time"		    % "joda-time" 		% "latest.integration",
  "org.joda" 			% "joda-convert" 	% "latest.integration",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  //"org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
  //"com.hp.hpl.jena" % "jena" % "2.6.4",
  //"com.hp.hpl.jena" % "arq" % "2.8.8"
  "log4j" % "log4j" % "1.2.14"
)

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "1.6.1", 
 "org.apache.spark" %% "spark-streaming" % "1.6.1", 
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1", 
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1" 
)

//libraryDependencies ++= Seq(
//"org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
// "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided",
//  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1" %  "provided",
//  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1" %  "provided"
//)

libraryDependencies += "org.fluttercode.datafactory" % "datafactory" % "0.8"

//libraryDependencies += "abdn" % "SimpleNLG" % "4.4.7-SNAPSHOT"

assemblySettings 

Revolver.settings

mainClass in Revolver.reStart := Some("tornado.ui.TornadoWebserver")

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Ywarn-dead-code",
  "-language:_",
  //"-target:jvm-1.6",
  "-encoding", "UTF-8"
)
