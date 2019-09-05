package version0

case class WriteCypher(cypher:String, isDistribute:Boolean)

case class CommitCypher(flag:Boolean)

case class ReadCypher(cypher: String)
