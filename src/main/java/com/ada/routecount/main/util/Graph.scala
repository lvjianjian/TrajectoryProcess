package com.ada.routecount.main.util

import com.ada.routecount.main.model.{VertexIndexAboutTraj, Point, Vertex}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

import scala.{Predef, collection}
import scala.collection.Map

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

  def loadVertexFromDataSource2(vertexFile: String): RDD[Vertex] = {
    logInfo("Loading Vertex data from %s".format(vertexFile))
    sc.textFile(vertexFile).map {
      x => val temp = x.split("\t")
        new Vertex(temp(0).toLong, new Point(temp(2).toDouble, temp(1).toDouble))
    }
  }

  override def loadEdgeFromDataSource(edgeFile: String): RDD[(Long, (Long, Long))] = {
    logInfo("Loading Edge data from %s".format(edgeFile))
    sc.textFile(edgeFile).map {
      x => val temp = x.split("\t")
        (temp(0).toLong, (temp(1).toLong, temp(2).toLong))
    }
  }

  /**
   * 从所有边获取开始点和所有以开始点为起点的所有边
   * @param edges_RDD
   * @return key vertexid value edgeids
   */
  def getStartVertexWithEdge(edges_RDD: RDD[(Long, (Long, Long))]): RDD[(Long, Iterable[Long])] = {
    edges_RDD.map({
      edge =>
        val edge_id = edge._1
        val start_vertex_id = edge._2.x._1
        (start_vertex_id, edge_id)
    }).groupByKey()
  }

  /**
   * 从所有边获取结束点和所有以该点为终点的所有边
   * @param edges_RDD
   * @return key vertexid value edgeids
   */
  def getEndVertexWithEdge(edges_RDD: RDD[(Long, (Long, Long))]): RDD[(Long, Iterable[Long])] = {
    edges_RDD.map({
      edge =>
        val edge_id = edge._1
        val end_vertex_id = edge._2.x._2
        (end_vertex_id, edge_id)
    }).groupByKey()
  }

  /**
   * 获取经过odpair的所有轨迹id
   * @param oid
   * @param did
   * @param start_with_edges_broadcast
   * @param end_with_edges_broadcast
   * @param edge_trajids_RDD
   * @return
   */
  def getAllTrajIds(oid: Long, did: Long,
                    start_with_edges_broadcast: Broadcast[collection.Map[Long, Iterable[Long]]],
                    end_with_edges_broadcast: Broadcast[collection.Map[Long, Iterable[Long]]],
                    edge_trajids_RDD: RDD[(Long, VertexIndexAboutTraj)]): Set[Long] = {
      getAllTrajIdsAndSite(oid,did,start_with_edges_broadcast,end_with_edges_broadcast,edge_trajids_RDD).map(x=>x._1).toSet
  }

  /**
   * 获取经过odpair的所有轨迹id和该轨迹的位置
   * @param oid
   * @param did
   * @param start_with_edges_broadcast
   * @param end_with_edges_broadcast
   * @param edge_trajids_RDD
   * @return
   */
  def getAllTrajIdsAndSite(oid: Long, did: Long,
                           start_with_edges_broadcast: Broadcast[collection.Map[Long, Iterable[Long]]],
                           end_with_edges_broadcast: Broadcast[collection.Map[Long, Iterable[Long]]],
                           edge_trajids_RDD: RDD[(Long, VertexIndexAboutTraj)]): List[(Long, (Int, Int))] = {
    val s_edgeids = start_with_edges_broadcast.value.get(oid).get
    val e_edgeids = end_with_edges_broadcast.value.get(did).get
    var s_set: Set[(Long, Int)] = Set()
    var e_set: Set[(Long, Int)] = Set()
    for (edgeid <- s_edgeids) {
      s_set = edge_trajids_RDD.lookup(edgeid)(0).getAllTrajs.toSet union (s_set)
    }

    for (edgeid <- e_edgeids) {
      e_set = edge_trajids_RDD.lookup(edgeid)(0).getAllTrajs.toSet union (e_set)
    }
    val join1: List[(Long, (Int, Int))] = join(s_set, e_set)
    join1.filter(x=>x._2.x._1 < x._2.x._2)

  }


  private def join(s_set: Set[(Long, Int)], e_set: Set[(Long, Int)]): List[(Long, (Int, Int))] = {
    val map: Predef.Map[Long, Int] = e_set.toMap
    var result: List[(Long, (Int, Int))] = Nil
    s_set.toMap.foreach({
      x =>
        val traj_id = x._1
        val site = x._2
        val end_site: Option[Int] = map.get(traj_id)
        if (end_site != None) {
          result = (traj_id, (site, end_site.get)) :: result
        }
    })

    result
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
    }.map(x => x.id).collect().toSet
  }

  //拿到周围相关的边 返回完全在区域内的边的ID
  def region_edge(graph_edge_RDD: RDD[(Long, (Long, Long))], iterable: Set[Long]): RDD[Long] = {
    graph_edge_RDD.filter { x =>
      iterable.contains(x._2._1) && iterable.contains(x._2._2)
    }.map(x => x._1)
  }

}
