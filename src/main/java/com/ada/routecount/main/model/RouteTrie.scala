package com.ada.routecount.main.model

import java.io._

import scala.collection.mutable
import scala.collection.mutable.HashSet


/**
 * Created by lzj on 2016/7/29.
 * Trie tree save routes
 */
class RouteTrie(start_vertex_id: Long) extends Serializable {
  val root: TrieNode = TrieNode(start_vertex_id)


  var destinations: mutable.HashMap[Long, mutable.HashSet[TrieNode]] = new mutable.HashMap[Long, mutable.HashSet[TrieNode]]()

  /**
   *
   * @param vertex_ids all vertex_ids in a path (contains start_vertex_id and end_vertex_id)
   */
  def addRoute(vertex_ids: List[Long]): Unit = {
    var current = root
    for (i <- 1 until (vertex_ids.length - 1)) {
      //add no-leafnode
      current = current.addChild(vertex_ids(i), false)
    }
    val end_vertex_id: Long = vertex_ids(vertex_ids.length - 1)
    current = current.addChild(end_vertex_id, true)
    val leafnodes: mutable.HashSet[TrieNode] = destinations.getOrElseUpdate(end_vertex_id, new mutable.HashSet[TrieNode]())
    if (!leafnodes.contains(current)) {
      current.vertex_ids = vertex_ids
      leafnodes.add(current)
    }

  }

  /**
   * get all routes which from start_vertex_id to end_vertex_id
   * @param end_vertex_id
   * @return
   */
  def getAllRoutesToDestination(end_vertex_id: Long): Array[Route] = {
    var routes: Array[Route] = null
    destinations.get(end_vertex_id) match {
      case None => routes = new Array[Route](0)
      case Some(leafnodes) =>
        routes = new Array[Route](leafnodes.size)
        var i = 0
        leafnodes.foreach({
          leafnode =>
            routes(i) = Route(leafnode.vertex_ids)
            routes(i).setFrequencyToZero()
            routes(i).addRouteFrequency(leafnode.freq)
            i += 1
        })
    }
    routes
  }


  @Deprecated
  def saveToFile(path: String): Unit = {
    val outObj = new ObjectOutputStream(new FileOutputStream(path + "/%d.obj".format(start_vertex_id)))
    outObj.writeObject(this)
  }



  def saveToXml(path:String): Unit ={

  }
}

object RouteTrie {
  def apply(start_vertex_id: Long) = new RouteTrie(start_vertex_id)

  /**
   * get route trie from file or return null if file not exist
   * @param filename
   * @return
   */
  @Deprecated
  def readFromFile(filename: String): RouteTrie = {
    try {
      val in = new ObjectInputStream(new FileInputStream(filename))
      val trie: RouteTrie = in.readObject().asInstanceOf[RouteTrie]
      trie
    } catch {
      case filenotexit: FileNotFoundException => return null
    }
  }


  def readFromXml(filename: String): RouteTrie = {
    try {
      val in = new ObjectInputStream(new FileInputStream(filename))
      val trie: RouteTrie = in.readObject().asInstanceOf[RouteTrie]
      trie
    } catch {
      case filenotexit: FileNotFoundException => return null
    }
  }

  def main(args: Array[String]) {
    val rt = RouteTrie(1)
    rt.addRoute(List(1, 2, 3, 4, 5))
    rt.addRoute(List(1, 2, 3, 4, 5))
    rt.addRoute(List(1, 2, 3, 5))
    rt.addRoute(List(1, 2, 3, 5))
    rt.addRoute(List(1, 2, 3, 5))
    rt.addRoute(List(1, 2, 3, 5))
    rt.addRoute(List(1, 2, 3, 2, 1, 5))
    rt.addRoute(List(1, 2, 3, 222, 5))
    rt.addRoute(List(1, 2, 3, 222, 4))
    rt.addRoute(List(1, 2, 3, 222, 4))
    rt.addRoute(List(1, 2, 3, 4))
    val allRoutes: Array[Route] = rt.getAllRoutesToDestination(4)
    println("===============================")
    rt.saveToFile("D:\\吕中剑\\OutlierDetection\\data\\wise2015_data\\")

    val trie: RouteTrie = readFromFile("D:\\吕中剑\\OutlierDetection\\data\\wise2015_data\\1.obj")
    if (trie == null) {
      println("null")
    } else {
      trie.getAllRoutesToDestination(4).foreach(println(_))
      rt.addRoute(List(1, 2, 3, 4))
      rt.saveToFile("D:\\吕中剑\\OutlierDetection\\data\\wise2015_data\\")
    }
  }
}
