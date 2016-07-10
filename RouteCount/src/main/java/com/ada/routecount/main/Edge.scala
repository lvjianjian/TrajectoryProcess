package com.ada.routecount.main

/**
 * Created by JQ-Cao on 2016/3/9.
 */
case class Edge(val id: Long, val start: Vertex, val end: Vertex) extends Serializable{


  def contain(vertex: Vertex): Boolean = {
    if (this.start.equals(vertex) || this.end.equals(vertex))
      true
    else
      false
  }

}
