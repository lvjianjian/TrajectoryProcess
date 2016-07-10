package com.ada.routecount.main

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

/**
 * Created by JQ-Cao on 2016/3/10.
 */
trait LoaderRoadNetwork {
  def loadVertexFromDataSource(vertexFile: String): RDD[Vertex]

  def loadEdgeFromDataSource(edgeFile: String): RDD[(Long, (Long, Long))]
}

class Graph(@transient val sc: SparkContext) extends Logging with LoaderRoadNetwork with Serializable {

  def mapLine2Vertex(line: String): Vertex = {
    val temp = line.split("\t")
    new Vertex(temp(0).toLong, new Point(temp(1).toDouble, temp(2).toDouble))
  }

  override def loadVertexFromDataSource(vertexFile: String): RDD[Vertex] = {
    logInfo("Loading Vertex data from %s".format(vertexFile))
    sc.textFile(vertexFile).map {
      x => mapLine2Vertex(x)
    }
  }

  override def loadEdgeFromDataSource(edgeFile: String): RDD[(Long, (Long, Long))] = {
    logInfo("Loading Edge data from %s".format(edgeFile))
    sc.textFile(edgeFile).map {
      x => val temp = x.split("\t")
        (temp(0).toLong, (temp(1).toLong, temp(2).toLong))
    }
  }

}

object Graph {
  //计算所有点的相关边
//  def region_all_vertex(vertex_broadcast: Broadcast[Map[Long, Vertex]]): Map[Long, Iterable[Vertex]] = {
//    vertex_broadcast.value.map {
//      x => val region_range = Tool.getAround(x._2.point, 1000)
//        (x._1, vertex_broadcast.value.filter(
//          y => y._2.point.includeOfRegion(region_range)
//        ).values)
//    }
//  }

  //拿到周围相关的点
  def region_vertex(vertex: Vertex, vertex_broadcast: RDD[Vertex]): Set[Long] = {
    val region_range = Tool.getAround(vertex.point, 500)
    vertex_broadcast.filter {
      x =>
        x.point.includeOfRegion(region_range)
    }.map(x=>x.id).collect().toSet
  }

  //拿到周围相关的边 返回完全在区域内的边的ID
  def region_edge(graph_edge_RDD: RDD[(Long, (Long, Long))], iterable: Set[Long]): RDD[Long] = {
    graph_edge_RDD.filter { x =>
      iterable.contains(x._2._1) && iterable.contains(x._2._2)
    }.map(x=>x._1)
  }
}
