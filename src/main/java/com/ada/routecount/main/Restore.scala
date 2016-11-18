package com.ada.routecount.main

import com.ada.routecount.main.model.Vertex
import com.ada.routecount.main.util.{Graph, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Created by JQ-Cao on 2016/8/1.
 */
/**
 * 还原朱杰的轨迹数据到经纬度，其余默认为0（速度，时间，方向）
 */
object Restore extends Logging {
  def main(args: Array[String]) {
    restoreData

  }



  def restoreData: Unit = {
    val conf = new SparkConf().setAppName("RestoreTrajOfZhujie")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)
    sc.textFile("/user/lvzhongjian/data/zhujie/lzj/data_vid.txt")
    val trajUtil = new Trajectory(sc)
    //get trajsByVertexes_RDD
    val trajsByVertexes_RDD: RDD[(Long, Array[Long],Array[Long])] = trajUtil.loadTrajectoryFromDataSource2(Parameters.HDFS_NODE_FRONT_PART + "/user/lvzhongjian/data/zhujie/lzj/data_vid.txt").cache()

    val graph = new Graph(sc)
    //get vertexes_RDD
    val vertex_RDD: RDD[Vertex] = graph.loadVertexFromDataSource2(Parameters.HDFS_NODE_FRONT_PART + "/user/lvzhongjian/data/zhujie/lzj/roadnetwork/vertices.txt")
    val vertexes_broadcast = sc.broadcast {
      vertex_RDD.map(v => (v.id, v.point)).collectAsMap()
    }


    trajsByVertexes_RDD.map({
      traj =>

        val traj_id = traj._1
        val vertex_ids = traj._2
        val times = traj._3
        var result = traj_id.toString
        for(i<- 0 until(vertex_ids.size)){
          val vertex_id = vertex_ids(i)
          val time = times(i)
          val point = vertexes_broadcast.value.get(vertex_id)
          result += ",%d|%d|0|%d|0".format((point.get.lng * 10000000).toLong, (point.get.lat * 10000000).toLong,time)
        }
        result
    }).coalesce(10).saveAsTextFile(Parameters.HDFS_BASE_RESULT_DIR + "restore_trajwithtime_data_zhujie")
  }
}