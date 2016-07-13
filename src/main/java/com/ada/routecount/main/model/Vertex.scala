package com.ada.routecount.main.model

/**
 * Created by JQ-Cao on 2016/3/9.
 */
case class Vertex(val id: Long, val point: Point) extends Serializable{

  private var is_visit:Int = 0
  private val adjacentEdges: List[Edge] = Nil
  //出度
  private var outEdges: List[Edge] = Nil
  //入度
  private var inEdges: List[Edge] = Nil

  def calculateInOut(): Unit = {
    if ((outEdges.size + inEdges.size) == 0) {
      for (edge: Edge <- adjacentEdges) {
        if (edge.start == this)
          outEdges = edge :: outEdges
        else
          inEdges = edge :: inEdges
      }
    }
  }

  def Equals(obj: Object): Boolean = {
    obj match {
      case Vertex(id,_) =>
        id == this.id
      case _ => false
    }
  }

  def Is_visit():Int = is_visit

  def set_is_visit(is_visit:Int): Unit ={
    this.is_visit = is_visit
  }
}
