package com.ada.routecount.main

import com.ada.routecount.main.model.Vertex
import com.ada.routecount.main.util.Graph
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lzj on 2016/8/15.
 */
object UnifyMap {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetODPairWithTrajIds")
    val sc = new SparkContext(conf)
    val graph = new Graph(sc)
    //get edges_RDD
    val vertex_rdd: RDD[Vertex] = graph.loadVertexFromDataSource(Parameters.vertex_data_path)
    //重复的vertex
    val vertexs_duplicate: Array[Seq[Long]] = vertex_rdd.map(x => ((x.point.lng + "," + x.point.lat), x.id)).groupByKey().filter(x => x._2.size > 1).map(x => x._2.toSeq).coalesce(1).collect()
    val vertex_dup_bc: Broadcast[Array[Seq[Long]]] = sc.broadcast(vertexs_duplicate)
    sc.textFile(Parameters.edge_data_path).map(
    { x => val temp = x.split("\t")
      (temp(0).toLong, temp(1).toLong, temp(2).toLong,temp(3).toDouble)
    }).map(x => {
      val edgeid = x._1
      var s_v_id = x._2
      var e_v_id = x._3
      vertex_dup_bc.value.foreach({
        ids =>
          if (ids.contains(s_v_id))
            s_v_id = ids.head
          if (ids.contains(e_v_id))
            e_v_id = ids.head

      })
      edgeid+"\t"+s_v_id+"\t"+e_v_id+"\t"+x._4
    }).coalesce(1).saveAsTextFile(Parameters.HDFS_BASE_RESULT_DIR+"new_edges")
  }
}
