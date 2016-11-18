package com.ada.routecount.main

import com.ada.routecount.main.model.VertexIndexAboutTraj
import com.ada.routecount.main.util.Trajectory
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Created by lzj on 2016/7/28.
 * build index like: vid:(trajid1,site1)...
 */

object BuildIndexAboutEdgeAndTraj extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BuildIndexAboutVertexAndTraj")
    //    conf.set("fs.hdfs.impl.disable.cache", "true")
    //    conf.setMaster("local")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)

    var start_time = System.currentTimeMillis()
    val trajUtil = new Trajectory(sc)
    //get trajsByEdges_RDD
    val trajsByEdges_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource(Parameters.traj_data_path).cache()
    var end_time = System.currentTimeMillis()
    logInfo("loadTrajectoryFromDataSource use time:" + (end_time - start_time))

    start_time = System.currentTimeMillis()
    //bulid index
    val index_RDD: RDD[VertexIndexAboutTraj] = trajsByEdges_RDD.flatMap({
      traj =>
        val traj_id = traj._1
        val edgeids = traj._2
        var r: List[(Long, (Long, Int))] = Nil
        edgeids.zipWithIndex.foreach({ x => r = (x._1, (traj_id, x._2)) :: r })
        r
    }).groupByKey().map {
      x =>
        val edge_id = x._1
        val trajsWithSite = x._2
        val vertexIndexAboutTraj = VertexIndexAboutTraj(edge_id)
        trajsWithSite.foreach(tws => vertexIndexAboutTraj.addTraj(tws._1, tws._2))
        vertexIndexAboutTraj
    }

    //    //save result
    //    val collect: Array[VertexIndexAboutTraj] = index_RDD.collect()
    //    end_time = System.currentTimeMillis()
    //    logInfo("bulid index spend time "+(end_time - start_time))
    //    start_time = System.currentTimeMillis()
    //    collect.foreach(x=>x.saveToFile(Parameters.INDEX_VERTEX_TRAJ_DIR))
    //    end_time = System.currentTimeMillis()
    //    logInfo("save result spend time" + (end_time - start_time))
    index_RDD.coalesce(6).saveAsTextFile(Parameters.HDFS_BASE_RESULT_DIR+"edge_index")
  }
}


