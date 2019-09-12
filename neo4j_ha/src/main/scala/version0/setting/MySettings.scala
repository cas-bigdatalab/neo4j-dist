package version0.setting

class MySettings {
  // Connect to your neo4j database
  val account = "neo4j"
  val password = "123123"

  // Connect to your Zookeeper service
  val connectString = "192.168.49.10:2181,192.168.49.11:2181,192.168.49.12:2181"
  val sessionTimeout = 2000

}
