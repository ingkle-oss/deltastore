import sbt.ExclusionRule

ThisBuild / parallelExecution := false

val jacksonVersion = sys.env.getOrElse("JACKSON_VERSION", "2.13.1")
val hadoopVersion = sys.env.getOrElse("HADOOP_VERSION", "3.3.5")
val parquetVersion = sys.env.getOrElse("PARQUET_VERSION", "1.12.3")
val parquet4sVersion = sys.env.getOrElse("PARQUET4S_VERSION", "1.9.4")
val deltaVersion = sys.env.getOrElse("DELTA_VERSION", "2.3.0")
val jreBaseImage = sys.env.getOrElse("JRE_BASE_IMAGE", "openjdk:17-jdk-slim")

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := "2.12.10",
  fork := true,
  Test / javaOptions ++= Seq(
    "-Dspark.ui.enabled=false",
    "-Dspark.ui.showConsoleProgress=false",
    "-Dspark.databricks.delta.snapshotPartitions=2",
    "-Dspark.sql.shuffle.partitions=5",
    "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
    "-Dspark.delta.sharing.network.sslTrustAll=true",
    "-Xmx1024m"
  )
)

lazy val jmxSettings = Seq(
  javaOptions ++= Seq(
    "-Djavax.management.builder.initial=",
    "-Djava.rmi.server.hostname=127.0.0.1",
    "-Dcom.sun.management.jmxremote=true",
    "-Dcom.sun.management.jmxremote.port=9186",
    "-Dcom.sun.management.jmxremote.rmi.port=9186",
    "-Dcom.sun.management.jmxremote.ssl=false",
    "-Dcom.sun.management.jmxremote.authenticate=false"
  )
)

lazy val root = (project in file(".")).aggregate(server)

lazy val server =
  (project in file("server")) enablePlugins (JavaAppPackaging) settings (
    name := "delta-store-server",
    commonSettings,
    jmxSettings,
    scalaStyleSettings,
    dockerRepository := sys.props.get("docker.repository"),
    dockerBuildOptions ++= Seq("--platform", "linux/amd64"),
    dockerBaseImage := jreBaseImage,
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion,
      "com.github.mjakubowski84" %% "parquet4s-core" % parquet4sVersion excludeAll (
        ExclusionRule("org.slf4j", "slf4j-api")
      ),
      "org.json4s" %% "json4s-jackson" % "3.5.3" excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module")
      ),
      "com.linecorp.armeria" %% "armeria-scalapb" % "1.6.0" excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("org.json4s")
      ),
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf" excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("org.json4s")
      ),
      "org.apache.hadoop" % "hadoop-aws" % hadoopVersion excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava")
      ),
      "org.apache.hadoop" % "hadoop-azure" % hadoopVersion excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava")
      ),
      "com.google.cloud" % "google-cloud-storage" % "2.2.2" excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module")
      ),
      "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-2.2.4" excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module")
      ),
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava")
      ),
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava")
      ),
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("com.google.guava", "guava")
      ),
      "io.delta" % "delta-storage" % deltaVersion,
      "org.apache.spark" %% "spark-sql" % "2.4.7" excludeAll (
        ExclusionRule("org.slf4j"),
        ExclusionRule("io.netty"),
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module"),
        ExclusionRule("org.json4s"),
        ExclusionRule("com.google.guava", "guava")
      ),
      "org.rocksdb" % "rocksdbjni" % "7.7.3",
      "io.lettuce" % "lettuce-core" % "6.2.1.RELEASE",
      "org.slf4j" % "slf4j-api" % "1.6.1",
      "org.slf4j" % "slf4j-simple" % "1.6.1",
      "net.sourceforge.argparse4j" % "argparse4j" % "0.9.0",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
    ),
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    )
  )

ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val scalaStyleSettings = Seq(
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  (Compile / compile) := ((Compile / compile) dependsOn compileScalastyle).value,
  testScalastyle := (Test / scalastyle).toTask("").value,
  (Test / test) := ((Test / test) dependsOn testScalastyle).value
)
