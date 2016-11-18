package com.ada.routecount.main.model

import scala.collection.mutable


/**
 * Created by lzj on 2016/7/29.
 * Trie node
 */
class TrieNode(_node_id: Long) extends Serializable{

  /**
   * 记录routeid
   */
  var route_id = -1

  /**
   * node_id save vertex_id
   */
  val node_id = _node_id
  /**
   * 0 is non-leafnode,other is leafnode
   */
  var freq = 0

  /**
   * leafnode save vertexids about the route
   */
  var vertex_ids: List[Long] = _

  val children: mutable.HashMap[Long, TrieNode] = new mutable.HashMap[Long, TrieNode]()

  /**
   * if it is a leafnode ,it records a route score
   * -1 is non-leafnode,other is leafnode
   */
  var score:Double = -1

  /**
   * 如果按点存储，即为node_id,按边存储，需要额外点信息
   */
  var end_vertex_id = node_id

  /**
   * addChild on this node,non thread safe
   * @param node_id
   * @param isEnd
   * @return child
   */
  def addChild(node_id: Long, isEnd: Boolean): TrieNode = {
    val child: TrieNode = children.getOrElseUpdate(node_id, TrieNode(node_id))
    if(isEnd)
      child.freq += 1
    child
  }

  def addChild(trieNode: TrieNode): Unit ={
    children.put(trieNode.node_id,trieNode)
  }


}

object TrieNode extends Serializable{
  def apply(vertex_id: Long) = new TrieNode(vertex_id)

}
