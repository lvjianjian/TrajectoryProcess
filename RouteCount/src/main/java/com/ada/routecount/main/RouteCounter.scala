package com.ada.routecount.main

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by lzj on 2016/7/10.
  */

/**
  * the main application to count route
  */
object RouteCounter extends Logging {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("RouteCounter").setMaster("local")
        val sc = new SparkContext(conf)

        val trajUtil = new Trajectory(sc)
        val traj_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource("")//traj data path

        val graph = new Graph(sc)
        val vertex_RDD: RDD[Vertex] = graph.loadVertexFromDataSource("")//vertex data path



    }
}

