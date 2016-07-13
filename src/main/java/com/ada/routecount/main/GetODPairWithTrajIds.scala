package com.ada.routecount.main
import java.io.{FileWriter, File}

import com.ada.routecount.main.util.{Graph, Trajectory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf, Logging}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by JQ-Cao on 2016/7/13.
 */

/**
 *  this get OD pair and it's all trajs which are going through ODPair
 */
object GetODPairWithTrajIds extends Logging {
  //    val traj_data_path: String = "D:\\lzj_spark_test\\small_tarjs.txt"
  //    val edge_data_path: String = "D:\\lzj_spark_test\\edges_new.txt"
  //    val route_result_path: String = "D:\\lzj_spark_test\\data\\route_result.txt"
  //    val route_path: String = "D:\\lzj_spark_test\\data\\route.txt"
  //    val od_pair_count_result: String = "D:\\lzj_spark_test\\data\\OD_Pair_Trajs.txt"
  val traj_data_path: String = "hdfs://node1:9000/user/caojiaqing/JqCao/data/trajectory_beijing_new.txt"
  val edge_data_path: String = "hdfs://node1:9000/user/caojiaqing/JqCao/data/edges_new.txt"
  val od_pair_count_result: String = "/home/liyang/lvzhongjian/result/OD_Pair_Trajs.txt"

  val conf = new SparkConf().setAppName("GetODPairWithTrajIds")
  //    conf.set("fs.hdfs.impl.disable.cache", "true")
  //    conf.setMaster("local[3]")
  conf.set("spark.driver.maxResultSize", "30g")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    var start_time =System.currentTimeMillis()
    val trajUtil = new Trajectory(sc)
    //get trajsByEdges_RDD
    val trajsByEdges_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource(traj_data_path)
    var end_time = System.currentTimeMillis()
    logInfo("loadTrajectoryFromDataSource use time:"+(end_time - start_time))

    val graph = new Graph(sc)
    //get edges_RDD
    val edges_RDD: RDD[(Long, (Long, Long))] = graph.loadEdgeFromDataSource(edge_data_path)
    val edges_broadcast: Broadcast[Map[Long, (Long, Long)]] = sc.broadcast(edges_RDD.collect().toMap)


    start_time =System.currentTimeMillis()
    //transfer traj's edge to vertex
    val trajsByVertexs_RDD: RDD[(Long, Iterable[Long])] = Trajectory.transferToVertex(trajsByEdges_RDD,edges_broadcast).cache()
    val trajsByVertexs: Array[(Long, Iterable[Long])] = trajsByVertexs_RDD.collect()
    val trajsByVertexs_broadcast: Broadcast[Array[(Long, Iterable[Long])]] = sc.broadcast(trajsByVertexs)
    end_time = System.currentTimeMillis()
    logInfo("transfer traj's edge to vertex use time:"+(end_time - start_time))


    //get different start_vertex and end_vertex group
    val distinct_s_e: RDD[(Long, Long)] = trajsByVertexs_RDD.map {
      traj =>
        val vertexs = traj._2.toArray
        val start_vertex_id = vertexs(0)
        val end_vertex_id = vertexs(vertexs.length - 1)
        (start_vertex_id, end_vertex_id)
    }.distinct()


    start_time = System.currentTimeMillis()
    //count each od pair's trajs which are going them
    val map: RDD[((Long, Long), Array[Long])] = distinct_s_e.map {
      x =>
        val start_id = x._1
        val end_id = x._2
        val traj_ids = new ArrayBuffer[Long]()
        trajsByVertexs_broadcast.value.foreach {//find traj  go through S and E vertex
          traj =>
            val traj_id = traj._1
            val vertexs = traj._2.toArray
            if (Trajectory.containsStartAndEndVertex(vertexs, start_id, end_id))
              traj_ids += traj_id
        }
        ((start_id, end_id), traj_ids.toArray)
    }
    end_time = System.currentTimeMillis()
    logInfo("count each od pair's trajs which are going them use time:"+(end_time - start_time))



    start_time = System.currentTimeMillis()
    val odwriter = new FileWriter(new File(od_pair_count_result))
    //save result
    map.collect().foreach(
      x => {
        val s_e_id: (Long, Long) = x._1
        //save (start_vertex_id,end_vertex_id)|
        odwriter.write(s_e_id+"|")
        //save traj's num|
        odwriter.write(x._2.length+"|")
        //save traj_id1,traj_id2,......
        x._2.foreach(trajid=>odwriter.write(trajid+","))
        odwriter.write("\r\n")
      }
    )
    odwriter.flush()
    odwriter.close()
    end_time = System.currentTimeMillis()
    logInfo("save result  use time:"+(end_time - start_time))

    sc.stop()
  }
  
}