package com.ada.routecount.main

import java.util

/**
  * Created by lzj on 2016/7/10.
  */
class Route(edge_ids:util.ArrayList[Long],start_vertex_id:Long,end_vertex_id:Long) {
    private var route_id:Long = 0
    private var frequency:Long = 0 //it record the frequency of the route
    private var edge_num = edge_ids.size()//it record the num of edges in this route
    private var expandRoutes:List[ExpandRoute] = Nil

    def setRouteId(routeid:Long): Unit ={
        route_id = routeid
    }
    def getRouteId():Long={route_id}

    /**
      * route num + 1
      */
    def count(){frequency += 1}
    def getRouteFrequency():Long = {frequency}

    def getEdgeNum():Int = {edge_num}


    def addExpandRoute(expandRoute: ExpandRoute): Unit ={
    }

    def getExpandRoutes()={
        expandRoutes
    }

}
object Route{
    def apply(edge_ids: util.ArrayList[Long], start_vertex_id: Long, end_vertex_id: Long): Route = new Route(edge_ids, start_vertex_id, end_vertex_id)
}
