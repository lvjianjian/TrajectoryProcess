package com.ada.routecount.main
import com.ada.routecount.main.model.{VertexIndexAboutTraj, Vertex}
import com.ada.routecount.main.util.{Graph, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf,Logging}

/**
 * Created by lzj on 2016/7/28.
 * build index like: vid:(trajid1,site1)...
 */

object BuildIndexAboutVertexAndTraj extends Logging{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BuildIndexAboutVertexAndTraj")
    //    conf.set("fs.hdfs.impl.disable.cache", "true")
//    conf.setMaster("local")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)

    var start_time = System.currentTimeMillis()
    val trajUtil = new Trajectory(sc)
    //get trajsByVertexes_RDD
    val trajsByVertexes_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource2(Parameters.HDFS_NODE_FRONT_PART+"/user/lvzhongjian/data/zhujie/lzj/data_vid.txt").cache()
    var end_time = System.currentTimeMillis()
    logInfo("loadTrajectoryFromDataSource use time:" + (end_time - start_time))

    val graph = new Graph(sc)
    //get vertexes_RDD
    val vertex_RDD: RDD[Vertex] = graph.loadVertexFromDataSource2(Parameters.HDFS_NODE_FRONT_PART+"/user/lvzhongjian/data/zhujie/lzj/roadnetwork/vertices.txt")

    start_time = System.currentTimeMillis()
    //bulid index
    val index_RDD: RDD[VertexIndexAboutTraj] = trajsByVertexes_RDD.flatMap({
      traj =>
        val traj_id = traj._1
        val vertexids = traj._2
        var r: List[(Long, (Long, Int))] = Nil
        vertexids.zipWithIndex.foreach({ x => r = (x._1, (traj_id, x._2)) :: r })
        r
    }).groupByKey().map {
      x =>
        val vertex_id = x._1
        val trajsWithSite = x._2
        val vertexIndexAboutTraj = VertexIndexAboutTraj(vertex_id)
        trajsWithSite.foreach(tws => vertexIndexAboutTraj.addTraj(tws._1, tws._2))
        vertexIndexAboutTraj
    }

    //save result
    val collect: Array[VertexIndexAboutTraj] = index_RDD.collect()
    end_time = System.currentTimeMillis()
    logInfo("bulid index spend time "+(end_time - start_time))
    start_time = System.currentTimeMillis()
    collect.foreach(x=>x.saveToFile(Parameters.INDEX_VERTEX_TRAJ_DIR))
    end_time = System.currentTimeMillis()
    logInfo("save result spend time" + (end_time - start_time))
  }
}


