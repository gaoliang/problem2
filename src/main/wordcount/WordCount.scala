//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
// 使用List代替python中的tuple类型
// 使用ArrayBuffer代替python中的list类型

object WordCount {

  //  val inputFile = "file:///Users/gaoliang/Downloads/WordCount/src/main/resources/bots_200_10.csv"
  //  val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
  //  val sc = new SparkContext(conf)
  //val lines: RDD[String] = sc.textFile(inputFile)


  // translate from python code
  var partitions = mutable.Map.empty[Set[Int], ArrayBuffer[ArrayBuffer[Long]]]
  var fds = ArrayBuffer.empty[Tuple2[Set[Int], Int]]
  var R = Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
  var RHS = mutable.Map(Set[Int]() -> R)

  def merge_partition(ps1: ArrayBuffer[ArrayBuffer[Long]],
                      ps2: ArrayBuffer[ArrayBuffer[Long]])
  : ArrayBuffer[ArrayBuffer[Long]] = {
    var s = ArrayBuffer.empty[ArrayBuffer[Long]]
    var iRow2p = mutable.Map.empty[Long, Int]
    for ((p1, i) <- ps1.zipWithIndex) {
      for (iRow <- p1) {
        iRow2p.update(iRow, i)
      }
    }

    for (p2 <- ps2) {
      var tmp = mutable.Map.empty[Long, ArrayBuffer[Long]]
      for (iRow <- p2) {
        if (!tmp.contains(iRow2p(iRow))) {
          tmp.put(iRow2p(iRow), ArrayBuffer.empty[Long])
        }
        tmp(iRow2p(iRow)) += iRow
      }
      s ++= tmp.values.to[ArrayBuffer]
    }
    return s

  }

  def generate_next_level(L: Set[Set[Int]]): Set[Set[Int]] = {
    var Ln = mutable.Set.empty[Set[Int]]
    for (l1 <- L) {
      for (l2 <- L) {
        if (l1 != l2 && (l1 -- l2).size == 1) {
          Ln.add(l1 | l2)
        }
      }
    }
    return Ln.toSet
  }

  def get_partition(attributes: Set[Int]): ArrayBuffer[ArrayBuffer[Long]] = {
    if (partitions.contains(attributes)) {
      return partitions(attributes)
    }
    if (attributes.isEmpty) {
      partitions.put(attributes, ArrayBuffer())
    }
    else if (attributes.size == 1) {
      var iAttr = attributes.toList.head
      var lines = Source.fromFile("/Users/gaoliang/Downloads/WordCount/src/main/resources/bots_200_10.csv").getLines()
      var table = lines.map(line => line.split(",", 0).toList)
      var d = mutable.Map.empty[String, ArrayBuffer[Long]]
      for ((row, index) <- table.zipWithIndex) {
        if (!d.contains(row(iAttr))) {
          d(row(iAttr)) = ArrayBuffer.empty[Long]
        }
        d(row(iAttr)) += index
      }
      partitions.put(attributes, d.values.to[ArrayBuffer])
    }
    else {
      var attr_tuple = attributes.toList
      var ps1 = get_partition(attr_tuple.slice(0, attr_tuple.size - 1).toSet)
      var ps2 = get_partition(attr_tuple.slice(0, attr_tuple.size - 2).toSet + attr_tuple.last)
      partitions.put(attributes, merge_partition(ps1, ps2))
    }
    return partitions(attributes)
  }

  def isValid(X: Set[Int], E: Int): Boolean = {
    return get_partition(X -- Set(E)).size == get_partition(X).size
  }

  def compute_dependencies(L: Set[Set[Int]]): Set[Set[Int]] = {
    //    var L_new = L.clone()
    var L_new = L.to[mutable.Set]
    for (bigX <- L) {
      RHS.put(bigX, R)
      for (bigE <- bigX) {
        RHS.put(bigX, RHS(bigX) & RHS(bigX -- Set(bigE)))
      }

      for (bigE <- RHS(bigX) & bigX) {
        if (isValid(bigX, bigE)) {

          fds += Tuple2[Set[Int], Int](bigX -- Set(bigE), bigE)
          RHS(bigX) --= Set(bigE)
          RHS(bigX) = RHS(bigX) & bigX
        }
      }
      if (RHS(bigX).isEmpty) {
        L_new.remove(bigX)
      }
    }
    println(L_new)
    L_new.toSet
  }

  def main(args: Array[String]) {
    var L = R.map(x => Set(x))
    L = compute_dependencies(L)
    for (i <- 0 to 9) {
      L = compute_dependencies(generate_next_level(L))
    }
    println(fds)

  }


}