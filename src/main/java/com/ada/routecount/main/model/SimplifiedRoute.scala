package com.ada.routecount.main.model

/**
 * Created by JQ-Cao on 2016/7/13.
 */
class SimplifiedRoute extends Serializable{
  var edge_ids :Array[Long]= _
  var vertex_ids:Array[Long] = _
  var start_vertex_id :Long= -1
  var end_vertex_id :Long= -1
  var route_id:Long = 0
  var frequency:Long = 1 //it record the frequency of the route
}

object SimplifiedRouteUtil{

}