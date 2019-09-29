package version1.Utils

object GetDistinctId{
  def main(args: Array[String]): Unit = {
    val t = "ahadf asdf wer qraaaaaaaaaaaaa"
    val n = new GetDistinctId
    println(n.getHash(t))
  }
}

// use FNV1_32_HASH algorithm
class GetDistinctId {
  def getHash(str: String): String = {
    val p = 16777619
    var hash = 2166136261L.toInt
    for (i <- 0 until str.length) {
      hash = (hash ^ str(i)) * p
      hash += hash << 13
      hash ^= hash >> 7
      hash += hash << 3
      hash ^= hash >> 17
      hash += hash << 5

    }
    if (hash < 0){
      hash = Math.abs(hash)
    }
    hash.toString
  }
}
