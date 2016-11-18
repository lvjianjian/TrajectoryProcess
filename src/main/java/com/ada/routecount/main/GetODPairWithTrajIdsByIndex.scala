package com.ada.routecount.main

import com.ada.routecount.main.model.Vertex
import com.ada.routecount.main.util.{Graph, Trajectory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection

/**
 * Created by lzj on 2016/7/13.
 */

/**
 * this get OD pair and it's all trajs which are going through ODPair
 * by index method
 */
object GetODPairWithTrajIdsByIndex extends Logging {
  val od_pair_count_result: String = "/home/liyang/lvzhongjian/result/OD_Pair_TrajsCount.txt"
  val od_pair_traj_result = "/home/liyang/lvzhongjian/result/OD_Pair_Trajs.txt"


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetODPairWithTrajIds")
    //    conf.set("fs.hdfs.impl.disable.cache", "true")
    //    conf.setMaster("local[3]")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)

    var start_time = System.currentTimeMillis()
    val trajUtil = new Trajectory(sc)
    //get trajsByEdges_RDD
    val trajsByEdges_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource(Parameters.traj_data_path).cache()
    var end_time = System.currentTimeMillis()
    logInfo("loadTrajectoryFromDataSource use time:" + (end_time - start_time))

    val graph = new Graph(sc)
    //get edges_RDD
    val edges_RDD: RDD[(Long, (Long, Long))] = graph.loadEdgeFromDataSource(Parameters.edge_data_path).cache()
    val edges_broadcast: Broadcast[Map[Long, (Long, Long)]] = sc.broadcast(edges_RDD.collect().toMap)

    //get vertexes_RDD
    val vertex_RDD: RDD[Vertex] = graph.loadVertexFromDataSource(Parameters.vertex_data_path)



    //get point and it's edges index


    start_time = System.currentTimeMillis()
    //get index about start_vertex_id and it's edgeid
    val start_with_edges_broadcast: Broadcast[collection.Map[Long, Iterable[Long]]] = sc.broadcast(graph.getStartVertexWithEdge(edges_RDD).collectAsMap())
    end_time = System.currentTimeMillis()
    log.info("build start_vertex and edges index spend time " + (end_time - start_time))

    //get index about end_vertex_id and it's edgeid
    start_time = System.currentTimeMillis()
    val end_with_edges_broadcast: Broadcast[collection.Map[Long, Iterable[Long]]] = sc.broadcast(graph.getEndVertexWithEdge(edges_RDD).collectAsMap())
    end_time = System.currentTimeMillis()
    log.info("build end_vertex and edges index spend time " + (end_time - start_time))

    //get index about edgeid and it's trajid
    start_time = System.currentTimeMillis()
    val edge_with_trajs_index: Broadcast[collection.Map[Long, Iterable[Long]]] = sc.broadcast(Trajectory.indexOfTrajectory(trajsByEdges_RDD).collectAsMap())
    end_time = System.currentTimeMillis()
    log.info("build edgeid and trajs index spend time " + (end_time - start_time))

    //  get odpair which it's count > 100
    val odpairs: List[(Long, Long)] = getODPairListExceedNum("hdfs://node1:9000/user/lvzhongjian/result/odPairCountResult/part-00000", sc, 100)
    println("count > 100 odpairs' size = " + odpairs.size)
    val odpairs_broadcast: Broadcast[List[(Long, Long)]] = sc.broadcast(odpairs)

    val odpair_with_all_trajs: List[(Int, (Long, Long), Set[Long])] = odpairs.map({
      odpair =>
        start_time = System.currentTimeMillis()
        val s_v_id = odpair._1
        val e_v_id = odpair._2
        val s_edgeids = start_with_edges_broadcast.value.get(s_v_id).get
        val e_edgeids = end_with_edges_broadcast.value.get(e_v_id).get
        var s_alltrajs: Set[Long] = Set()
        var e_alltrajs: Set[Long] = Set()
        s_edgeids.foreach({ s_edge_id => s_alltrajs = edge_with_trajs_index.value.get(s_edge_id).get.toSet union s_alltrajs })
        e_edgeids.foreach(e_edge_id => e_alltrajs = edge_with_trajs_index.value.get(e_edge_id).get.toSet union e_alltrajs)
        end_time = System.currentTimeMillis()
        log.info("handle " + odpair + " spend time " + (end_time - start_time))
        val alltrajids: Set[Long] = s_alltrajs.intersect(e_alltrajs)
        (alltrajids.size, odpair, alltrajids)
    })
    sc.parallelize(odpair_with_all_trajs).repartition(1).saveAsTextFile(Parameters.HDFS_BASE_RESULT_DIR + "odpairWithAlltrajsByIndex")
    sc.stop()
  }


  /**
   * count od pair's num,and sort by desc
   * @param s_e_RDD RDD[(s_id,e_id)]
   * @return RDD[(s_id,e_id),num]
   */
  def countODPair(s_e_RDD: RDD[(Long, Long)]): RDD[((Long, Long), Int)] = {
    s_e_RDD.map(x => (x, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
  }


  /**
   *
   * @param odPair_count_result_path  RDD.saveAsTextFilePath + /{part-XXXXX} path, RDD should be RDD[((Long,Long),Int)] which to save by saveAsTextFilePath
   * @param sc SparkContext
   * @param num minimum count num
   * @return
   */
  def getODPairListExceedNum(odPair_count_result_path: String, sc: SparkContext, num: Int): List[(Long, Long)] = {
    val odPair_count_RDD: RDD[String] = sc.textFile(odPair_count_result_path)
    val regex = """\(\(([0-9]+),([0-9]+)\),([0-9]+)\)""".r
    var s_e_list: List[(Long, Long)] = Nil
    odPair_count_RDD.collect() foreach {
      line =>
        //        println(line)
        val regex(s_id, e_id, count) = line
        //        println(s_id,e_id,count)
        if (count.toInt > num) {
          s_e_list = (s_id.toLong, e_id.toLong) :: s_e_list
        }
    }
    s_e_list
  }

  /**
   * get all odpair RDD
   * @param odPair_count_result_path
   * @param sc
   * @return
   */
  def getAllODPair(odPair_count_result_path: String, sc: SparkContext): RDD[(Long, Long)] = {
    val odPair_count_RDD: RDD[String] = sc.textFile(odPair_count_result_path)
    val regex = """\(\(([0-9]+),([0-9]+)\),([0-9]+)\)""".r
    odPair_count_RDD.map {
      line =>
        //        println(line)
        val regex(s_id, e_id, count) = line
        //        println(s_id,e_id,count)
        (s_id.toLong, e_id.toLong)
    }
  }

}