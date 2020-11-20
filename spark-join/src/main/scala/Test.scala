import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level,Logger}

/**
  * Created by $maoshuwu on 2020/11/2.
  */

object Test {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("join").setMaster("local[2]")
    val sc = new SparkContext(conf)



    val rdd1 = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    val rdd2 = sc.makeRDD(List((1, "a1"), (1, "a2"), (4, "d1")))

    print("join: " + rdd1.join(rdd2).collect().toList)

    print("leftOuterJoin: " + rdd1.leftOuterJoin(rdd2).collect().toList)

    print("rightOuterJoin: " + rdd1.rightOuterJoin(rdd2).collect().toList)

    print("fullOuterJoin: " + rdd1.fullOuterJoin(rdd2).collect().toList)

  }

}
