package com.ada.routecount.main

/**
  * Created by lzj on 2016/7/10.
  * it record a route's expandroute ,which include the route
  */
class ExpandRoute(start_vertex_id:Long,end_vertex_id:Long,route_id:Long) {}

object ExpandRoute{
    def apply(start_vertex_id: Long, end_vertex_id: Long, route_id: Long): ExpandRoute = new ExpandRoute(start_vertex_id, end_vertex_id, route_id)
}
