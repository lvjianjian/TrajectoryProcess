/**
 * Created by lzj on 2016/7/14.
 */
package com.ada.routecount.main

import com.ada.routecount.main.model.Route
import com.ada.routecount.main.util.{Graph, Trajectory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext, SparkConf}

object GetODPairWithRoutes extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetODPairWithTrajIds")
    //    conf.set("fs.hdfs.impl.disable.cache", "true")
    //    conf.setMaster("local[3]")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)

    var start_time = System.currentTimeMillis()
    val trajUtil = new Trajectory(sc)
    //get trajsByEdges_RDD(node:face with too large data,we don't collect,otherwise it could cause oom)
    val trajsByEdges_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource(Parameters.traj_data_path).cache()
    //    val trajsByEdges_collect: Map[Long, Array[Long]] = trajsByEdges_RDD.collect().toMap

    val graph = new Graph(sc)
    //get edges_RDD
    val edges_RDD: RDD[(Long, (Long, Long))] = graph.loadEdgeFromDataSource(Parameters.edge_data_path)
    //broadcast to each computer node
    val edges_broadcast: Broadcast[Map[Long, (Long, Long)]] = sc.broadcast(edges_RDD.collect().toMap)

    //get odpairWithtrajs from file
    val regex = """\(([0-9]+),\(\(([0-9]+),([0-9]+)\),CompactBuffer\((.*)\)\)\)""".r
    val odpairWithTrajIds_RDD: RDD[((Long, Long), Array[Long])] = sc.textFile(Parameters.HDFS_NODE_FRONT_PART +
      Parameters.HDFS_BASE_RESULT_DIR + "odpairWithAlltrajs/part-*").map {
      line =>
        val regex(num, s_id, e_id, traj_ids) = line
        val splits: Array[String] = traj_ids.split(", ") //get all traj_ids
      var trajid_list: List[Long] = Nil
        val edges = edges_broadcast.value
        val s_idL = s_id.toLong
        val e_idL = e_id.toLong
        splits.foreach({
          trajid =>
            val id = trajid.toLong //convert to Long
            trajid_list = id :: trajid_list
        })
        ((s_idL, e_idL), trajid_list.toArray)
    }

    //get odpairWithRoute from odpairWithtrajs
    val odpairWithRoute_RDD: RDD[((Long, Long), Iterable[Route])] = sc.parallelize(odpairWithTrajIds_RDD.collect().map {
      x =>
        var route_list: List[Route] = Nil
        val s_e = x._1
        val se_broadcast: Broadcast[(Long, Long)] = sc.broadcast(s_e)
        val trajids = x._2
        val trajids_broadcast: Broadcast[Array[Long]] = sc.broadcast(trajids)
        //choose all traj which are related to odpair
        val filter_RDD: RDD[(Long, Array[Long])] = trajsByEdges_RDD.filter(x => trajids_broadcast.value.contains(x._1))
        //transfer trajs to routes
        filter_RDD.collect().foreach({
          x =>
            route_list = Route(Trajectory.getSubTraj(x._2, s_e._1, s_e._2, edges_broadcast.value), s_e._1, s_e._2) :: route_list
        })
        //transfer trajs to routes
        //        val collect: Array[Route] = filter_RDD.map({
        //          x =>
        //            Route(Trajectory.getSubTraj(x._2, se_broadcast.value._1, se_broadcast.value._2, edges_broadcast.value), se_broadcast.value._1, se_broadcast.value._2)
        //        }).collect()
        //        print(collect.size)
        //        se_broadcast.unpersist()
        trajids_broadcast.unpersist()
        (s_e, route_list.toIterable)
    })

    //merge route for each odpair's routes
    val odpairWithmergeRouteRDD: RDD[((Long, Long), Iterable[Route])] = Route.mergeRoute(odpairWithRoute_RDD)


    odpairWithmergeRouteRDD.map {
      x =>
        var result: List[(Long,String)] = Nil
        val s_e = x._1
        val routes = x._2
        routes.foreach { route => result = (route.getRouteFrequency(), "edge_id:"+route.edge_ids.mkString(",")) :: result }
        (s_e,routes.size,result)
    }.repartition(1).saveAsTextFile(Parameters.HDFS_BASE_RESULT_DIR+"odpair_with_routes")

    edges_broadcast.unpersist()
    sc.stop()
  }
}
