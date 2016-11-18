package com.ada.routecount.main.model

import java.io._
import java.util
import scala.collection.JavaConversions._
import org.dom4j.io.{SAXReader, XMLWriter, OutputFormat}
import org.dom4j._

import scala.collection.mutable
import scala.collection.mutable.HashSet


/**
 * Created by lzj on 2016/7/29.
 * Trie tree save routes
 */
class RouteTrie(start_vertex_id: Long) extends Serializable {

  //用于给route编号,同时记录数量
  var count = 0;
  var root: TrieNode = TrieNode(start_vertex_id)
  var destinations: mutable.HashMap[java.lang.Long, mutable.HashSet[TrieNode]] = new mutable.HashMap[java.lang.Long, mutable.HashSet[TrieNode]]()
  /**
   * true表示只针对一个目的地
   */
  var single: Boolean = false

  /**
   * single为true时d_id存储目的地的点id
   */
  var d_id: java.lang.Long = _

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
      count += 1
      current.route_id = count
      leafnodes.add(current)
    }
  }


  def setSingle(d_id: Long): Unit = {
    this.single = true
    this.d_id = d_id
  }


  def getTriNodesOfDesitination(d_id: Long): util.Set[TrieNode] = {
    setAsJavaSet(destinations.get(d_id).get)
  }


  /**
   *
   * @param edge_ids all vertex_ids in a path (contains start_vertex_id and end_vertex_id)
   */
  def addRouteByEdge(edge_ids: List[Long], end_vertex_id: Long): Unit = {
    var current = root
    for (i <- 0 until (edge_ids.length - 1)) {
      //add no-leafnode
      current = current.addChild(edge_ids(i), false)
    }
    val end_edge_id: Long = edge_ids(edge_ids.length - 1)
    current = current.addChild(end_edge_id, true)
    val leafnodes: mutable.HashSet[TrieNode] = destinations.getOrElseUpdate(end_vertex_id, new mutable.HashSet[TrieNode]())
    if (!leafnodes.contains(current)) {
      current.vertex_ids = edge_ids
      current.end_vertex_id = end_vertex_id
      count += 1
      current.route_id = count
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


  /**
   * get all routes(edgeids) which from start_vertex_id to end_vertex_id
   * @param end_vertex_id
   * @return
   */
  def getAllRoutesByEdgesToDestination(end_vertex_id: Long): Array[Route] = {
    var routes: Array[Route] = null
    destinations.get(end_vertex_id) match {
      case None => routes = new Array[Route](0)
      case Some(leafnodes) =>
        routes = new Array[Route](leafnodes.size)
        var i = 0
        leafnodes.foreach({
          leafnode =>
            routes(i) = Route(leafnode.vertex_ids.toArray, root.node_id, end_vertex_id)
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


  def saveToXml(path: String): Unit = {
    val doc: Document = DocumentHelper.createDocument()
    val queue_node: mutable.Queue[TrieNode] = new mutable.Queue[TrieNode]()
    val queue_element: mutable.Queue[Element] = new mutable.Queue[Element]()

    queue_node.enqueue(root)
    queue_element.enqueue(createElementByTrieNode(root, doc))
    queue_element.get(0).get.addAttribute("single", single.toString)
    if (single == true)
      queue_element.get(0).get.addAttribute("did", d_id.toString)
    while (!queue_node.isEmpty) {
      val current_node: TrieNode = queue_node.dequeue()
      val current_element: Element = queue_element.dequeue()
      current_node.children.values.foreach({
        node =>
          queue_node.enqueue(node)
          queue_element.enqueue(createElementByTrieNode(node, current_element))
      })
    }

    //实例化输出格式对象
    val format: OutputFormat = OutputFormat.createPrettyPrint();
    //设置输出编码
    format.setEncoding("UTF-8");
    var filename: String = ""
    if (single == false)
      filename = path + "/%d.xml".format(start_vertex_id)
    else
      filename = path + "/%d_%d.xml".format(start_vertex_id, d_id)
    println("save " + filename)
    //创建需要写入的File对象
    val file: File = new File(filename);
    //生成XMLWriter对象，构造函数中的参数为需要输出的文件流和格式
    val writer: XMLWriter = new XMLWriter(new FileOutputStream(file), format);
    //开始写入，write方法中包含上面创建的Document对象
    writer.write(doc);
    writer.flush()

  }

  private def createElementByTrieNode(node: TrieNode, father: Branch): Element = {
    val element: Element = father.addElement("node")
    element.addAttribute("id", node.node_id.toString)
    if (node.freq > 0) {
      element.addAttribute("freq", node.freq.toString)
      element.addAttribute("endvid", node.end_vertex_id.toString)
    }
    if (node.score > -1)
      element.addAttribute("score", node.score.toString)
    if (node.vertex_ids != null)
      element.addAttribute("vertex_ids", node.vertex_ids.mkString(","))
    if(node.route_id > 0)
      element.addAttribute("routeid",node.route_id.toString)
    element
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


  /**
   * get route trie from xml file or return null if file not exist
   * @param filename
   * @return
   */
  def readFromXml(filename: String): RouteTrie = {
    var f = "";
    try {
      if (!filename.contains(".xml")) {
        f = filename + ".xml"
      } else {
        f = filename
      }
      //read xml
      val reader: SAXReader = new SAXReader();
      val file = new File(f);
      val document = reader.read(file);
      //create queue
      val queue_node: mutable.Queue[TrieNode] = new mutable.Queue[TrieNode]()
      val queue_element: mutable.Queue[Element] = new mutable.Queue[Element]()
      //get root element
      val rootElement: Element = document.getRootElement()
      val rt = new RouteTrie(rootElement.attribute("id").getValue.toLong)
      val singleAttr: Attribute = rootElement.attribute("single")
      var single: Boolean = false
      if (singleAttr != null)
        single = singleAttr.getValue.toBoolean
      if (single == true) {
        rt.setSingle(rootElement.attribute("did").getValue.toLong)
      }
      val _destinations: mutable.HashMap[java.lang.Long, mutable.HashSet[TrieNode]] = new mutable.HashMap[java.lang.Long, mutable.HashSet[TrieNode]]()
      queue_element.enqueue(rootElement)
      val root_node: TrieNode = createTrieNodeFromElement(rootElement)
      queue_node.enqueue(root_node)
      //bulid trie
      while (!queue_element.isEmpty) {
        val current_element: Element = queue_element.dequeue()
        val curent_node: TrieNode = queue_node.dequeue()
        val iterator: util.Iterator[_] = current_element.elements().iterator()
        while (iterator.hasNext) {
          val element: Element = iterator.next().asInstanceOf[Element]
          queue_element.enqueue(element)
          val node: TrieNode = createTrieNodeFromElement(element)
          curent_node.addChild(node)
          queue_node.enqueue(node)
          if (node.freq > 0) {
            val set: mutable.HashSet[TrieNode] = _destinations.getOrElseUpdate(node.end_vertex_id, new mutable.HashSet[TrieNode]())
            set.add(node)
          }
        }
      }


      //set root and destinations
      rt.root = root_node
      rt.destinations = _destinations
      rt
    } catch {
      case e: Exception => println(e) ;return null
    }
  }

  private def createTrieNodeFromElement(element: Element): TrieNode = {
    val node = TrieNode(element.attribute("id").getValue.toLong)
    val freq_attr: Attribute = element.attribute("freq")
    val score_attr: Attribute = element.attribute("score")
    val routeid_attr = element.attribute("routeid")
    if (freq_attr != null) {
      node.freq = freq_attr.getValue.toInt
      node.vertex_ids = element.attribute("vertex_ids").getValue.split(",").map(id => id.toLong).toList
      val endvid: Attribute = element.attribute("endvid")
      if(endvid != null)
        node.end_vertex_id = endvid.getValue.toLong
    }
    if (score_attr != null) {
      node.score = score_attr.getValue.toDouble
    } else {
      node.score = -1
    }
    if(routeid_attr != null){
      node.route_id = routeid_attr.getValue.toInt
    }
    node
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
    rt.addRoute(List(1, 2, 4, 3))
    rt.addRoute(List(1, 3, 5, 4))
    //    rt.saveToXml("D:\\吕中剑\\OutlierDetection\\data\\wise2015_data\\lzj")
    val rt_read: RouteTrie = readFromXml("D:\\吕中剑\\OutlierDetection\\data\\wise2015_data\\lzj\\1.xml")
    rt_read.getAllRoutesToDestination(5).foreach(route => println(route))
    rt_read.getAllRoutesToDestination(4).foreach(route => println(route))
    rt_read.getAllRoutesToDestination(3).foreach(route => println(route))
  }
}
