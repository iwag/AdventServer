scalaVersion := "2.10.4"

organization := "com.github.iwag"

name := "adventservice"

com.twitter.scrooge.ScroogeSBT.newSettings

libraryDependencies ++= Seq(
  "com.twitter"    %% "twitter-server"        % "1.8.0",
  "com.twitter"    %% "finagle-stats"         % "6.22.0",
  "com.twitter"    %% "util-jvm"              % "6.22.0",
  "org.scalatest"  %  "scalatest_2.10"        % "2.2.1" % "test"
)


resolvers += "twitter" at "http://maven.twttr.com"
