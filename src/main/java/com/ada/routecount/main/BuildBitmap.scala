/**
 * Created by lzj on 2016/9/14.
 */

package com.ada.routecount.main

import java.io.FileWriter

import com.ada.routecount.main.model.Vertex
import com.ada.routecount.main.util.{Trajectory, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf, Logging}

object BuildBitmap extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BuildBitmap")
    val sc = new SparkContext(conf)
    logWarning("BuildBitmap starting")
    val graph = new Graph(sc)
    val vertex_RDD: RDD[Vertex] = graph.loadVertexFromDataSource(Parameters.vertex_data_path)
    val trajUtil = new Trajectory(sc)
    val trajByEdges_RDD = trajUtil.loadTrajectoryFromDataSource(Parameters.traj_data_path).cache()
//    logWarning("traj size = " + trajByEdges_RDD.count()) //总共 5660692 条轨迹

    val verteices = vertex_RDD.collect()
    logWarning("verteices size = " + verteices.size)
    val trajsByVertexs_RDD = Trajectory.transferToByVertex(trajByEdges_RDD, sc.broadcast(graph.loadEdgeFromDataSource(Parameters.edge_data_path).collect().toMap))
    val fw = new FileWriter("/home/liyang/lvzhongjian/result/bitmap/bitmap.txt")
//    verteices.foreach({
//      vertex_id=>
//        println(vertex_id)
//        val bm = new java.util.BitSet()
//        //找到所有包含该点的轨迹id
//        trajsByVertexs_RDD.filter(x => x._2.toList.contains(vertex_id)).map(_._1).collect().foreach({
//          id =>
//            if (id < Int.MaxValue)
//              bm.set(id.toInt, true)
//            else
//              logError(id + "exceed Integer Max_value")
//        })
//        if(bm.cardinality() > 0){
//          //存在轨迹通过该点，保存
//          fw.write(vertex_id+"|"+bm.toString+"\r\n")
//        }
//    })

    trajsByVertexs_RDD.flatMap({
      traj=>
        val traj_id = traj._1
        val vertexids = traj._2
        var r: List[(Long,Long)] = Nil
        vertexids.foreach({ x => r = (x,traj_id) :: r })
        r
    }).groupByKey().sortByKey(true).map(({
      x=>
        val vertexid = x._1
        val trajids = x._2
        var string = x._1+"|"
        trajids.foreach(x=>string+=x+",")
        string
    })).coalesce(100).saveAsTextFile(Parameters.HDFS_BASE_RESULT_DIR+"bitmap_100_new")

  }
}