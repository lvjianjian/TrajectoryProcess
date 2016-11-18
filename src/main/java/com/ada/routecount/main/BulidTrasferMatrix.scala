package com.ada.routecount.main

import com.ada.routecount.main.model.{VertexIndexAboutTraj, Vertex}
import com.ada.routecount.main.util.{Trajectory, Graph}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.Map

/**
 * Created by lzj on 2016/8/17.
 */

/**
 * 根据destination构造转移矩阵
 */
object BulidTrasferMatrix extends Logging {


  def main(args: Array[String]) {
    val dids: Array[Long] = Array("59567301461".toLong,"59567303675".toLong,"60560412767".toLong,"60560415877".toLong)


    val conf = new SparkConf().setAppName("BulidTrasferMatrix")
    val sc = new SparkContext(conf)
    logWarning("BulidTrasferMatrix starting")
    val graph = new Graph(sc)
    val edges_RDD: RDD[(Long, (Long, Long))] = graph.loadEdgeFromDataSource(Parameters.edge_data_path)
    val edges_bc: Broadcast[Map[Long, (Long, Long)]] = sc.broadcast(edges_RDD.collectAsMap())
    val vertex_RDD: RDD[Vertex] = graph.loadVertexFromDataSource(Parameters.vertex_data_path)
    val end_with_edges = graph.getEndVertexWithEdge(edges_RDD).collectAsMap()
    val edgeIndexAboutTraj_RDD: RDD[VertexIndexAboutTraj] = sc.textFile(Parameters.HDFS_BASE_RESULT_DIR + "edge_index/part*").map(string => VertexIndexAboutTraj.getVertexIndexAboutTrajFromString(string)).cache()
    val edge_trajids_RDD: RDD[(Long, VertexIndexAboutTraj)] = edgeIndexAboutTraj_RDD.map(x => (x.vertex_id, x)).cache()
    val trajUtil = new Trajectory(sc)
    //get trajsByEdges_RDD
    val trajsByEdges_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource(Parameters.traj_data_path).cache()

    dids.foreach({
      did =>
        logWarning("BulidTrasferMatrix for %d".format(did))
        //找到所有以该点为目的地的边
        val all_edgeids_about_destination = end_with_edges.get(did)
        val all_edgeids_set: Set[Long] = all_edgeids_about_destination.get.toSet
        //从索引中找到这些边以及对应的索引,然后获取所有的边和边上的结束位置
        val collect = edge_trajids_RDD.filter({ x => all_edgeids_set.contains(x._1) }).flatMap({
          x =>
            x._2.getAllTrajs
        }).collectAsMap()
        logWarning("desitination id = %d,find trajs num = %d".format(did, collect.size))
        val broadcast = sc.broadcast(collect)

        trajsByEdges_RDD.filter(x => broadcast.value.keySet.contains(x._1)).flatMap({
          x =>
            val site: Int = broadcast.value.get(x._1).get
            var r: List[(Long, Long, Long)] = Nil //(s_id,e_id,edgeid)
            for (i <- 0 until x._2.length if (i <= site)) {
              val edgeid: Long = x._2(i)
              val s_e_vid: (Long, Long) = edges_bc.value.get(edgeid).get
              r = (s_e_vid._1, s_e_vid._2, edgeid) :: r
            }
            r
        }).map(x => (x, 1)).reduceByKey(_ + _).map({
          x =>
            x._1.productIterator.mkString(",")+","+x._2
        }).coalesce(1).saveAsTextFile(Parameters.HDFS_BASE_RESULT_DIR+"transfermatrix_%d".format(did))
        broadcast.unpersist()
    })


    logWarning("BulidTrasferMatrix stoping")
    edges_bc.unpersist()
    sc.stop()

  }
}