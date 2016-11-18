package com.ada.routecount.main

import com.ada.routecount.main.GetODPairWithTrajIdsByIndex._
import com.ada.routecount.main.model.{Route, RouteTrie, VertexIndexAboutTraj, Vertex}
import com.ada.routecount.main.util.{Trajectory, Graph}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf, Logging}

import scala.collection
import scala.collection.mutable

object IBAT {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GetODPairWithTrajIds")
    //    conf.set("fs.hdfs.impl.disable.cache", "true")
    conf.setMaster("local[3]")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)
    val v1: VertexIndexAboutTraj = VertexIndexAboutTraj(1)
    v1.addTraj(1, 0)
    v1.addTraj(3, 0)
    val v2: VertexIndexAboutTraj = VertexIndexAboutTraj(2)
    v2.addTraj(1, 1)
    v2.addTraj(2, 1)
    v2.addTraj(3, 1)
    val v3: VertexIndexAboutTraj = VertexIndexAboutTraj(3)
    v3.addTraj(1, 2)
    v3.addTraj(2, 2)
    val v4: VertexIndexAboutTraj = VertexIndexAboutTraj(4)
    v4.addTraj(4, 0)
    val v5: VertexIndexAboutTraj = VertexIndexAboutTraj(5)
    v5.addTraj(4, 1)
    val v6: VertexIndexAboutTraj = VertexIndexAboutTraj(6)
    v6.addTraj(4, 2)
    val v7: VertexIndexAboutTraj = VertexIndexAboutTraj(7)
    v7.addTraj(3, 2)
    val v8: VertexIndexAboutTraj = VertexIndexAboutTraj(8)
    v8.addTraj(2, 0)
    val ibat = new IBAT(Set(1, 2, 3, 4), 1, 256, sc.parallelize(List(v1, v2, v3, v4, v5, v6, v7, v8)).map(x => (x.vertex_id, x)))
    ibat.score(Array(new java.lang.Long("1"), new java.lang.Long(2),new java.lang.Long(3)))
  }


}


/**
 * Created by lzj on 2016/8/10.
 */
class IBAT {
  //包含odpair的所有轨迹id
  var all_traj_ids: Set[Long] = _
  //测试次数
  var trial_number = 0;
  //抽取的样例轨迹id数量
  var sub_sample_size = 0;
  //边和经过此边的所有轨迹id索引,RDD
  var edge_trajids_RDD: RDD[(Long, VertexIndexAboutTraj)] = _
  //边和经过此边的所有轨迹id索引,Map
  var edge_trajids_map:collection.Map[Long,VertexIndexAboutTraj] = _

  var use_map = false

  def this(trajids: Set[Long], trialnumber: Int, sub_sample_size: Int, edge_trajids_RDD: RDD[(Long, VertexIndexAboutTraj)]) {
    this()
    all_traj_ids = trajids
    trial_number = trialnumber
    this.sub_sample_size = sub_sample_size
    this.edge_trajids_RDD = edge_trajids_RDD
  }

  //结束ibat算法的轨迹数
  val EndSize: Int = 3
  //最大抽取边次数
  val MaxTestCount:Int = 60

  def score(test_traj: Array[Long]): Double = {
    if (all_traj_ids == None || edge_trajids_RDD == None || trial_number == 0 || sub_sample_size == 0)
      throw new Exception(s"parameter error!")
    println("score: " + test_traj.mkString(","))
    var all_count: Double = 0
    for (i <- 0 until trial_number) {
      var count = 0
      var sample_trajids: Set[Long] = randomSamples
      //      println(sample_trajids.mkString(","))
      do {
        count = count + 1

        val edge_id: Long = test_traj((math.random * test_traj.length).toInt)
        //        println("choose edge_id:" + edge_id)
        if(!use_map)
          sample_trajids = sample_trajids.intersect(edge_trajids_RDD.lookup(edge_id)(0).getAllTrajs.keySet)
        else{
          sample_trajids = sample_trajids.intersect(edge_trajids_map.get(edge_id).get.getAllTrajs.keySet)
        }
        //        println(sample_trajids.mkString(","))
        //        println(sample_trajids.size)
        if (count == MaxTestCount)
          sample_trajids = Set()
      } while (sample_trajids.size > EndSize)
      println("count:" + count)
      all_count += count
    }
    val c: Double = 2 * H(sub_sample_size - 1) - 2 * (sub_sample_size - 1) / sub_sample_size.toDouble
    math.pow(2, -((all_count / trial_number.toDouble) / c))
  }


  private def randomSamples(): Set[Long] = {
    var s: Set[Long] = Set()
    while (s.size < sub_sample_size) {
      val list: List[Long] = all_traj_ids.toList
      s = s + list((math.random * all_traj_ids.size).toInt)
    }
    s
  }

  private def H(i: Int): Double = {
    math.log1p(i - 1) + 0.57721566 //logip(x) = ln(x+1)
  }

}

object IBATDriver extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetODPairWithTrajIds")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)
    val graph = new Graph(sc)
    //get edges_RDD
    val edges_RDD: RDD[(Long, (Long, Long))] = graph.loadEdgeFromDataSource(Parameters.edge_data_path).cache()
    val edges_broadcast: Broadcast[Map[Long, (Long, Long)]] = sc.broadcast(edges_RDD.collect().toMap)

    //get vertexes_RDD
    val vertex_RDD: RDD[Vertex] = graph.loadVertexFromDataSource(Parameters.vertex_data_path)

    //get index about start_vertex_id and it's edgeid
    val start_with_edges_broadcast: Broadcast[collection.Map[Long, Iterable[Long]]] = sc.broadcast(graph.getStartVertexWithEdge(edges_RDD).collectAsMap())

    //get index about end_vertex_id and it's edgeid
    val end_with_edges_broadcast: Broadcast[collection.Map[Long, Iterable[Long]]] = sc.broadcast(graph.getEndVertexWithEdge(edges_RDD).collectAsMap())


    val od_ids = List(
      ("59566202194".toLong,"59566302333".toLong),
      ("59567302492".toLong,"59567301135".toLong),("59567302492".toLong,"60560415877".toLong),
      ("59565313370".toLong,"59565221575".toLong),("60560414814".toLong,"60560415595".toLong),
      ("60560414814".toLong,"60560415592".toLong), ("59566201082".toLong,"59566201416".toLong)
//                      ("60560416648".toLong,"59567301461".toLong),
//                      ("59565313370".toLong,"59565219109".toLong),("59566210844".toLong,"59567303675".toLong),
//                      ("60560416648".toLong,"60560412767".toLong),("59566310447".toLong,"59567303675".toLong),
//                      ("59567303823".toLong,"60560415877".toLong), ("60560415578".toLong,"59567301461".toLong)
                     )

    val edgeIndexAboutTraj_RDD: RDD[VertexIndexAboutTraj] = sc.textFile(Parameters.HDFS_BASE_RESULT_DIR + "edge_index/part*").map(string => VertexIndexAboutTraj.getVertexIndexAboutTrajFromString(string)).cache()
    val edge_trajids_RDD: RDD[(Long, VertexIndexAboutTraj)] = edgeIndexAboutTraj_RDD.map(x => (x.vertex_id, x)).cache()
    val trajUtil = new Trajectory(sc)
    //get trajsByEdges_RDD
    val trajsByEdges_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource(Parameters.traj_data_path).cache()

    for (i<-0 until(od_ids.size)) {

      val o_id = od_ids(i)._1
      val d_id = od_ids(i)._2


      val trajsAndSE_Site: Map[Long, (Int, Int)] = graph.getAllTrajIdsAndSite(o_id, d_id, start_with_edges_broadcast, end_with_edges_broadcast, edge_trajids_RDD).toMap


      //得到所有从开始点到终点的轨迹（包括了子轨迹）
      val alltrajsInODPair = trajsByEdges_RDD.filter(x => trajsAndSE_Site.contains(x._1)).map({
        x =>
          val traj_id = x._1
          val edgeids = x._2
          val site = trajsAndSE_Site.get(traj_id)
          val start_site = site.get._1
          val end_site = site.get._2
          val sub_edgeids = for (i <- 0 until (edgeids.length) if (i >= start_site); if (i <= end_site)) yield edgeids(i)
          (sub_edgeids)
      }).collect()


      //1.先找到经过odpair的所有轨迹上的所有边的edge_trajids索引，怕边太多时内存溢出
      var edgeids_set :mutable.Set[Long] = new mutable.HashSet[Long]()

      //save to routetrie
      val rt = RouteTrie(o_id)
      rt.setSingle(d_id)
      alltrajsInODPair.foreach({
        edgeids =>
          if(edgeids.size < 400) {// 过大在保存成xml时会栈溢出，暂时设置为最大400条
            rt.addRouteByEdge(edgeids.toList, d_id)
            edgeids_set = edgeids_set.++(edgeids.toList)
          }else{
            println(edgeids,"it's size is "+ edgeids.size)
          }
      })

      val edge_trajids_Map: collection.Map[Long, VertexIndexAboutTraj] = edge_trajids_RDD.filter(x=>edgeids_set.contains(x._1)).collectAsMap()
      val ibat = new IBAT(trajsAndSE_Site.keySet, 100, 256, edge_trajids_RDD)
      ibat.use_map = true
      ibat.edge_trajids_map = edge_trajids_Map


      //2.直接传入edge_trajids_RDD,速度慢
      //val ibat = new IBAT(trajsAndSE_Site.keySet, 100, 256, edge_trajids_RDD)


      rt.destinations.get(d_id).get.foreach({
        node =>
          val score: Double = ibat.score(node.vertex_ids.toArray)
          node.score = score
          println("last score:" + score)
      })

      rt.saveToXml("/home/liyang/lvzhongjian/result/routetree_new_400")
    }

    sc.stop()
  }
}