package com.ada.routecount.main
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

/**
 * Created by JQ-Cao on 2016/7/28.
 */

object CountODPair{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CountODPair")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)
    val odpair_map_RDD: RDD[(Long, Long)] = sc.textFile(Parameters.HDFS_NODE_FRONT_PART+"/user/lvzhongjian/data/data_vid.txt").map({
      line =>
        val strings = line.split(",")
        val s_id = strings(2)
        val e_id = strings(strings.length - 1)
        (s_id.toLong, e_id.toLong)
    })


    val od_pairs_counted_RDD: RDD[((Long, Long), Int)] = countODPair(odpair_map_RDD).cache()
    println(od_pairs_counted_RDD.count())
    od_pairs_counted_RDD.repartition(1).saveAsTextFile(Parameters.HDFS_BASE_RESULT_DIR+"odpaircount_zhujie");
  }

  /**
   * count od pair's num,and sort by desc
   * @param s_e_RDD RDD[(s_id,e_id)]
   * @return RDD[(s_id,e_id),num]
   */
  def countODPair(s_e_RDD: RDD[(Long, Long)]):RDD[((Long,Long),Int)] = {
    s_e_RDD.map(x => (x,1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
  }
}
