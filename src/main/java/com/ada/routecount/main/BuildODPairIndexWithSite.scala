package com.ada.routecount.main

import com.ada.routecount.main.model.VertexIndexAboutTraj
import com.ada.routecount.main.util.Graph
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf, Logging}

/**
 * Created by lzj on 2016/10/8.
 */
object BuildODPairIndexWithSite extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetODPairWithTrajIds")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)
    val graph = new Graph(sc)
    //get edges_RDD
    val edges_RDD: RDD[(Long, (Long, Long))] = graph.loadEdgeFromDataSource(Parameters.edge_data_path).cache()
    val edges_broadcast: Broadcast[Map[Long, (Long, Long)]] = sc.broadcast(edges_RDD.collect().toMap)
    val edgeIndexAboutTraj_RDD: RDD[VertexIndexAboutTraj] = sc.textFile(Parameters.HDFS_BASE_RESULT_DIR + "edge_index/part*").map(string => VertexIndexAboutTraj.getVertexIndexAboutTrajFromString(string)).cache()
    val edge_trajids_RDD: RDD[(Long, VertexIndexAboutTraj)] = edgeIndexAboutTraj_RDD.map(x => (x.vertex_id, x)).cache()
    val od_ids = List(
      ("60560416648".toLong, "59567301461".toLong),
      ("59565313370".toLong, "59565219109".toLong), ("59566210844".toLong, "59567303675".toLong),
      ("60560416648".toLong, "60560412767".toLong), ("59566310447".toLong, "59567303675".toLong),
      ("59567303823".toLong, "60560415877".toLong), ("60560415578".toLong, "59567301461".toLong)
    )

    for (i <- 0 until (od_ids.size)) {

      val o_id = od_ids(i)._1
      val d_id = od_ids(i)._2

      //get index about start_vertex_id and it's edgeid
      val start_with_edges_broadcast: Broadcast[collection.Map[Long, Iterable[Long]]] = sc.broadcast(graph.getStartVertexWithEdge(edges_RDD).collectAsMap())

      //get index about end_vertex_id and it's edgeid
      val end_with_edges_broadcast: Broadcast[collection.Map[Long, Iterable[Long]]] = sc.broadcast(graph.getEndVertexWithEdge(edges_RDD).collectAsMap())
      graph.getAllTrajIdsAndSite(o_id, d_id, start_with_edges_broadcast, end_with_edges_broadcast, edge_trajids_RDD)


    }

  }
}
