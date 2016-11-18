package com.ada.routecount.main.model

import java.io.{FileWriter, File}

import breeze.io.TextReader.FileReader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

/**
 * Created by JQ-Cao on 2016/7/28.
 * also can use in edge
 */
class VertexIndexAboutTraj(_vertex_id: Long) extends Serializable {
  val vertex_id = _vertex_id
  private var trajs: mutable.Map[Long, Int] = new mutable.HashMap[Long, Int]();

  /**
   * add traj which contains the vertex
   * @param traj_id
   * @param site site is the vertex  in the traj's position,start from 0
   * @return
   */
  def addTraj(traj_id: Long, site: Int): VertexIndexAboutTraj = {
    trajs.put(traj_id, site)
    this
  }


  def getAllTrajs: mutable.Map[Long, Int] = {
    trajs
  }


  /**
   * save to newfile ,save it's info to file,filename = path/{v_id}.txt
   * @param path
   */
  def saveToFile(path: String) {
    if (trajs.size == 0)
      return
    val f = new File(path + "/" + vertex_id + ".txt")
    val fw = new FileWriter(f)
    trajs.foreach({
      traj =>
        val trajid = traj._1
        val site = traj._2
        fw.write(trajid + "," + site + "\r\n")
    })
    fw.close()
  }


  override
  def toString(): String = {
    var r: String = vertex_id + "|"
    getAllTrajs.foreach({
      x =>
        val traj_id = x._1
        val site = x._2
        r = r + traj_id + "," + site + ","
    })
    return r
  }
}

object VertexIndexAboutTraj {
  def apply(vertex_id: Long) = new VertexIndexAboutTraj(vertex_id)

  /**
   * get a VertexIndexAboutTraj from path/{v_id}.txt
   * @param path
   * @param vertex_id
   * @return
   */
  def getFromFile(path: String, vertex_id: Long): VertexIndexAboutTraj = {
    val source: BufferedSource = Source.fromFile(path + "/" + vertex_id + ".txt")
    val vertexIndex = VertexIndexAboutTraj(vertex_id)
    for (line <- source.getLines()) {
      val split: Array[String] = line.toString.split(",")
      if (split.size == 2)
        vertexIndex.addTraj(split(0).toLong, split(1).toInt)
    }
    vertexIndex
  }


  /**
   *
   * @param path
   * @param vertex_id
   * @param sc
   * @return RDD[trajid,site]
   */
  def getTrajIdAndSiteRDDOfVertex(path:String,vertex_id:Long,sc:SparkContext):RDD[(Long,Int)] = {
    sc.textFile(path + "/" + vertex_id + ".txt").map({
      line =>
        val split: Array[String] = line.toString.split(",")
        (split(0).toLong, split(1).toInt)
    })
  }

  /**
   * @param fileName
   * @param sparkContext
   * @return RDD[VertexIndexAboutTraj]
   */
  def getAllIndexs_RDD(fileName: String, sparkContext: SparkContext): RDD[VertexIndexAboutTraj] = {
    sparkContext.textFile(fileName).map({
      line =>
        val strings: Array[String] = line.split("\\|")
        val vertexid = strings(0).toLong
        val indexAboutTraj = VertexIndexAboutTraj(vertexid)
        if (strings(1) != None && strings(1) != "") {
          val trajsWithSite: Array[String] = strings(1).split(",")
          //          println(strings(1)+","+trajsWithSite.size)
          for (i <- 0 until (trajsWithSite.length) if i % 2 == 0) {
            indexAboutTraj.addTraj(trajsWithSite(i).toLong, trajsWithSite(i + 1).toInt)
          }
        }
        indexAboutTraj
    })
  }

  /**
   * @param fileName
   * @param sparkContext
   * @return RDD[(vertexid, VertexIndexAboutTraj)]
   */
  def getAllIndexsWithVID_RDD(fileName: String, sparkContext: SparkContext): RDD[(Long, VertexIndexAboutTraj)] = {
    getAllIndexs_RDD(fileName, sparkContext).map(x => (x.vertex_id, x))
  }


  def getVertexIndexAboutTrajFromString(s:String): VertexIndexAboutTraj ={
    val split: Array[String] = s.split("\\|")
    val ve: VertexIndexAboutTraj = VertexIndexAboutTraj(split(0).toLong)
    val trajidAndSite: Array[String] = split(1).split(",")
    for (i <-0 until (trajidAndSite.length,2)){
      ve.addTraj(trajidAndSite(i).toLong,trajidAndSite(i+1).toInt)
    }
    ve
  }

  def main(args: Array[String]) {
//    val vertexIndexAboutTraj: VertexIndexAboutTraj = getFromFile("D:\\吕中剑\\OutlierDetection\\data\\wise2015_data\\lzj\\index_result", 44414)
//    println(vertexIndexAboutTraj)
//    val vertexIndexAboutTraj2: VertexIndexAboutTraj = getFromFile("D:\\吕中剑\\OutlierDetection\\data\\wise2015_data\\lzj\\index_result", 30171)
//    println(vertexIndexAboutTraj2)
    val conf = new SparkConf().setAppName("BuildIndexAboutVertexAndTraj")
    conf.setMaster("local")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)
//    println(getAllIndexs_RDD("file:\\D:\\吕中剑\\part-000*", sc).count())
    val vertex: RDD[(Long, Int)] = getTrajIdAndSiteRDDOfVertex("D:\\吕中剑\\OutlierDetection\\data\\wise2015_data\\lzj\\index_result",29896,sc)
    val vertex1: RDD[(Long, Int)] = getTrajIdAndSiteRDDOfVertex("D:\\吕中剑\\OutlierDetection\\data\\wise2015_data\\lzj\\index_result",29916,sc)
    vertex.join(vertex1)
      .filter({
      x =>
        val start_site = x._2.x._1
        val end_site = x._2.x._2
        start_site < end_site
    }).map(_._1).foreach(x=>println(x))

  }

}
