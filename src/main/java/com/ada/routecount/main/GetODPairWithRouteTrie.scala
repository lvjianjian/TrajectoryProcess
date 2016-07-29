package com.ada.routecount.main

import com.ada.routecount.main.model.{Route, RouteTrie, VertexIndexAboutTraj, Vertex}
import com.ada.routecount.main.util.{Graph, Trajectory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Created by lzj on 2016/7/13.
 */

/**
 * this get OD pair and it's route trie
 * by index method,index use VertexIndexAboutTraj
 */
object GetODPairWithRouteTrie extends Logging {
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
    val trajsByVertexes_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource2(Parameters.HDFS_NODE_FRONT_PART + "/user/lvzhongjian/data/zhujie/lzj/data_vid.txt").cache()
    var end_time = System.currentTimeMillis()
    logInfo("loadTrajectoryFromDataSource use time:" + (end_time - start_time))

    //  get odpair which it's count > 100
    val odpairs: List[(Long, Long)] = getODPairListExceedNum("hdfs://node1:9000/user/lvzhongjian/result/odpaircount_zhujie/part-00000", sc, 800)
    odpairs.foreach({
      odpair =>
        val s_id = odpair._1
        val e_id = odpair._2
        println("start:%d to %d".format(s_id,e_id))
        val s_indexs = VertexIndexAboutTraj.getTrajIdAndSiteRDDOfVertex("hdfs://node1:9000/user/lvzhongjian/data/zhujie/index_vertex_traj_zhujie", s_id, sc)
        val e_indexs = VertexIndexAboutTraj.getTrajIdAndSiteRDDOfVertex("hdfs://node1:9000/user/lvzhongjian/data/zhujie/index_vertex_traj_zhujie", e_id, sc)
        //intersection and start_vertex's site should be less than end_vertex's site
        val collect = s_indexs.join(e_indexs)
          .filter({
          x =>
            val start_site = x._2.x._1
            val end_site = x._2.x._2
            start_site < end_site
        }).map({
          x =>
            val traj_id = x._1
            val start_site = x._2.x._1
            val end_site = x._2.x._2
            (traj_id, (start_site, end_site))
        }).collectAsMap()
        //get traj
        val alltrajsInODPair = trajsByVertexes_RDD.filter(x => collect.keySet.contains(x._1)).map({
          x =>
            val traj_id = x._1
            val vertexids = x._2
            val site = collect.get(traj_id)
            val start_site = site.get._1
            val end_site = site.get._2
            val sub_vertexids = for (i <- 0 until (vertexids.length) if (i >= start_site); if (i <= end_site)) yield vertexids(i)
            (sub_vertexids)
        }).collect()
        println("alltrajs size:"+alltrajsInODPair.length)
        //save to routetrie
        var rt = RouteTrie.readFromFile("/home/liyang/lvzhongjian/result/routetree_zhujie/%d.obj".format(s_id))
        if(rt == null)
          rt = RouteTrie(s_id)
        alltrajsInODPair.foreach(vertexids=>rt.addRoute(vertexids.toList))
        val routes: Array[Route] = rt.getAllRoutesToDestination(e_id)
        var sum:Long = 0
        routes.foreach(route=>sum+=route.getRouteFrequency())
        rt.saveToFile("/home/liyang/lvzhongjian/result/routetree_zhujie")
        println("%d to %d,all trajs size is %d".format(s_id,e_id,sum))
    })
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