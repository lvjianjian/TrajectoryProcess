package com.ada.routecount.main

import com.ada.routecount.main.util.Trajectory
import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.rdd.RDD

/**
 * Created by JQ-Cao on 2016/10/10.
 */

object GetLengthOfTrajectory extends Logging{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetODPairWithTrajIds")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)
    val trajUtil = new Trajectory(sc)
    //get trajsByEdges_RDD
    val trajsByEdges_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource(Parameters.traj_data_path)
    trajsByEdges_RDD.map(x=>(x._2.size,1)).reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey().map(x=>(x._2,x._1)).coalesce(1)
      .saveAsTextFile(Parameters.HDFS_BASE_RESULT_DIR+"GetLengthOfTrajectory")
  }


}