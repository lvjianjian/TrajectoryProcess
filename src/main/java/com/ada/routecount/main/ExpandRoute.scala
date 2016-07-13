package com.ada.routecount.main

/**
  * Created by lzj on 2016/7/10.
  * it record a route's expandroute ,which include the route
  */
class ExpandRoute(_start_vertex_id:Long,_end_vertex_id:Long,_route_id:Long) extends Serializable{
    var start_vertex_id =_start_vertex_id
    var end_vertex_id = _end_vertex_id
    var route_id=_route_id
    override def toString: String = {
        start_vertex_id+","+end_vertex_id+","+route_id+"|"
    }
}

object ExpandRoute{
    def apply(start_vertex_id: Long, end_vertex_id: Long, route_id: Long): ExpandRoute = new ExpandRoute(start_vertex_id, end_vertex_id, route_id)
}
