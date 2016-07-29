package com.ada.routecount.main.model

import com.ada.routecount.main.util.Trajectory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by lzj on 2016/7/10.
 */
class Route(_start_vertex_id: Long, _end_vertex_id: Long) extends Serializable {
  var edge_ids: Array[Long] = _
  var vertex_ids: Array[Long] = _
  var start_vertex_id = _start_vertex_id
  var end_vertex_id = _end_vertex_id
  private var route_id: Long = 0
  private var frequency: Long = 1
  //it record the frequency of the route
  private var edge_num = -1
  //it record the num of edges in this route
  private var expandRoutes: List[ExpandRoute] = Nil




  def this(_edge_ids: Array[Long], _start_vertex_id: Long, _end_vertex_id: Long){
    this(_start_vertex_id,_end_vertex_id)
    edge_ids = _edge_ids
  }


  def this(_edge_ids: Array[Long], _start_vertex_id: Long, _end_vertex_id: Long, _vertex_ids: Array[Long]) {
    this(_edge_ids, _start_vertex_id, _end_vertex_id)
    vertex_ids = _vertex_ids
  }

  def this(_vertex_ids: List[Long]){
    this(_vertex_ids(0),_vertex_ids(_vertex_ids.length-1))
    vertex_ids = _vertex_ids.toArray
  }

  def setRouteId(routeid: Long): Unit = {
    route_id = routeid
  }

  def getRouteId(): Long = {
    route_id
  }

  /**
   * route frequency + 1
   */
  def count() {
    addRouteFrequency(1)
  }

  def setFrequencyToZero(): Unit = {
    frequency = 0
  }

  /**
   * set frequency = route old frequency + addfrequency_value
   * @param addfrequency_value
   */
  def addRouteFrequency(addfrequency_value: Long): Unit = {
    frequency += addfrequency_value
  }

  def getRouteFrequency(): Long = {
    frequency
  }

  def getEdgeNum(): Int = {
    if(edge_num != -1)
      return edge_num
    if(edge_ids != null)
      edge_num = edge_ids.length
    if(vertex_ids != null)
      edge_num = vertex_ids.length -1
    edge_num
  }


  def addExpandRoute(expandRoute: ExpandRoute): Unit = {
    expandRoutes = expandRoute :: expandRoutes
  }

  def getExpandRoutes() = {
    expandRoutes
  }

  /**
   * judge self and route is the same route,if the edges are same ,return true,otherwise return false
   * judge by edges
   * @param route
   * @return
   */
  def isSameRoute(route: Route): Boolean = {
    //the edge num is not same return false
    if (getEdgeNum() != route.getEdgeNum())
      return false
    val edges1 = this.edge_ids
    val edges2: Array[Long] = route.edge_ids
    //the edge num is same
    for (i <- 0 until edges1.length) {
      //exist one edge is not same,return false
      if (edges1(i) != edges2(i))
        return false
    }
    //all edges are same
    return true
  }

  /**
   * judge route whether contains sub route
   * judge by edges
   * @param sub_edge_ids
   * @return contains sub_route return true otherwise false
   */
  def containsSubRoute(sub_edge_ids: Array[Long]): Boolean = {
    if (getEdgeNum() < sub_edge_ids.length)
      return false
    //get first edge's index in route
    val first_edge_index: Int = edge_ids.indexOf(sub_edge_ids(0))
    if (first_edge_index == -1)
      return false
    else if (first_edge_index + sub_edge_ids.length > getEdgeNum())
      return false
    else {
      for (i <- 0 until (sub_edge_ids.length)) {
        if (sub_edge_ids(i) != edge_ids(first_edge_index + i))
          return false
      }
    }
    true
  }


  /**
   * judge the route whether contains vertex
   * @param vertex_id
   * @return
   */
  def containsVertex(vertex_id: Long): Boolean = {
    if (vertex_ids == null)
      throw new RuntimeException("vertex_ids is null,you can't call this function")
    vertex_ids.foreach {
      id => if (id == vertex_id)
        return true
    }
    false
  }


  /**
   * judge route whether contains start and end vertex(direction into consideration)
   * @param start_vertex_id
   * @param end_vertex_id
   * @return
   */
  def containsStartAndEndVertex(start_vertex_id: Long, end_vertex_id: Long): Boolean = {
    if (vertex_ids == null)
      throw new RuntimeException("vertex_ids is null,you can't call this function")
    Trajectory.containsStartAndEndVertex(vertex_ids, start_vertex_id, end_vertex_id)
  }


  override def toString: String = {
    "Route[id:%d,frequency:%d,edges_num:%d,start_vertex_id:%d,end_vertex_id:%d]"
      .format(getRouteId(), getRouteFrequency(), getEdgeNum(), _start_vertex_id, _end_vertex_id)
  }
}

object Route {

  def apply(vertex_ids:List[Long]) = new Route((vertex_ids))

  def apply(edge_ids: Array[Long], start_vertex_id: Long, end_vertex_id: Long): Route = new Route(edge_ids, start_vertex_id, end_vertex_id)

  def apply(_edge_ids: Array[Long], _start_vertex_id: Long, _end_vertex_id: Long, _vertex_ids: Array[Long]): Route = new Route(_edge_ids, _start_vertex_id, _end_vertex_id, _vertex_ids)

  /**
   * transform trajs to routes
   * @param trajs_RDD RDD[traj_id,Array(edge_ids)]
   * @param edges_broadcast Map[RDD(edge_id,(start_vertex_id,end_vertex_id))]
   * @return RDD[(start_vertex_id,end_vertex_id),Route]
   */
  def transformToRoutesFromTrajs(trajs_RDD: RDD[(Long, Array[Long])], edges_broadcast: Broadcast[Map[Long, (Long, Long)]]): RDD[((Long, Long), Route)] = {
    trajs_RDD.map {
      traj =>
        val edges_ids: Array[Long] = traj._2
        val start_vertex_id = edges_broadcast.value(edges_ids(0))._1
        val end_vertex_id = edges_broadcast.value(edges_ids(edges_ids.length - 1))._2
        val vertex_ids = ArrayBuffer[Long]()
        edges_ids.foreach {
          //get start_vertex of each edge
          edge_id =>
            vertex_ids += edges_broadcast.value(edge_id)._1
        }
        //add the last i.e. end-vertex_id
        vertex_ids += end_vertex_id
        val route = Route(edges_ids, start_vertex_id, end_vertex_id, vertex_ids.toArray)
        route.setRouteId(traj._1)
        ((start_vertex_id, end_vertex_id), route)
    }
  }


  /**
   * merge route
   * @param routes_grouped_RDD route has grouped by (s_v_id,e_v_id)
   * @return RDD[(s_v_id,e_v_id),Array(merged_routes)]
   */
  def mergeRoute(routes_grouped_RDD: RDD[((Long, Long), Iterable[Route])]): RDD[((Long, Long), Iterable[Route])] = {
    routes_grouped_RDD.map {
      x =>
        var merged_routes: List[Route] = Nil //merged_routes save route which are merged
        val routes: Iterable[Route] = x._2
        routes.map {
          route => var hasFound = false
            merged_routes.foreach {
              //find which route is same as route
              merged_route =>
                if (!hasFound) {
                  //if not find ,
                  if (merged_route.isSameRoute(route)) {
                    //has found
                    hasFound = true
                    merged_route.addRouteFrequency(route.getRouteFrequency())
                  }
                }
            }
            if (!hasFound) {
              //no route in mergedroutes is same as this route
              merged_routes = route :: merged_routes
            }
        }
        (x._1, merged_routes.toIterable)
    }
  }

  /**
   * reset route id for each route
   * @param route_merged_RDD route group which has merged ,group by (s_v_id,e_v_id)
   * @return route group which has merged and each route has been reset route id
   */
  def resetRouteId(route_merged_RDD: RDD[((Long, Long), Iterable[Route])]): RDD[((Long, Long), Iterable[Route])] = {
    val routeWithId_merged_RDD: RDD[((Long, Long), Iterable[Route])] = route_merged_RDD.flatMap {
      x =>
        val key = x._1
        val routes = x._2
        routes.map {
          route =>
            (key, route)
        }
    }.zipWithIndex().map {
      x => val routeWithS_E_V_ID = x._1
        val index = x._2
        routeWithS_E_V_ID._2.setRouteId(index)
        routeWithS_E_V_ID
    }.groupByKey()
    routeWithId_merged_RDD
  }


  def main(args: Array[String]) {
    val r = new Route(1,2)
    println(r.edge_ids)
  }
}
