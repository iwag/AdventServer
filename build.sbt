scalaVersion := "2.10.4"

organization := "com.github.iwag"

name := "adventservice"

com.twitter.scrooge.ScroogeSBT.newSettings

libraryDependencies ++= Seq(
  "com.twitter"    %% "twitter-server"        % "1.8.0"
)


resolvers += "twitter" at "http://maven.twttr.com"
