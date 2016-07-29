package com.ada.routecount.main
import java.io.{FileWriter, File}

import com.ada.routecount.main.util.{Graph, Trajectory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf, Logging}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by lzj on 2016/7/13.
 */

/**
 *  this get OD pair and it's all trajs which are going through ODPair
 */
object GetODPairWithTrajIds extends Logging {
//  val traj_data_path: String = "D:\\lzj_spark_test\\small_tarjs.txt"
//  val edge_data_path: String = "D:\\lzj_spark_test\\edges_new.txt"
//  val route_result_path: String = "D:\\lzj_spark_test\\data\\route_result.txt"
//  val route_path: String = "D:\\lzj_spark_test\\data\\route.txt"
//  val od_pair_count_result: String = "D:\\lzj_spark_test\\data\\OD_Pair_Trajs.txt"

  val od_pair_count_result: String = "/home/liyang/lvzhongjian/result/OD_Pair_TrajsCount.txt"
  val od_pair_traj_result = "/home/liyang/lvzhongjian/result/OD_Pair_Trajs.txt"



  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetODPairWithTrajIds")
//    conf.set("fs.hdfs.impl.disable.cache", "true")
//    conf.setMaster("local[3]")
    conf.set("spark.driver.maxResultSize", "30g")
    val sc = new SparkContext(conf)

    var start_time =System.currentTimeMillis()
    val trajUtil = new Trajectory(sc)
    //get trajsByEdges_RDD
    val trajsByEdges_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource(Parameters.traj_data_path)
    var end_time = System.currentTimeMillis()
    logInfo("loadTrajectoryFromDataSource use time:"+(end_time - start_time))

    val graph = new Graph(sc)
    //get edges_RDD
    val edges_RDD: RDD[(Long, (Long, Long))] = graph.loadEdgeFromDataSource(Parameters.edge_data_path)
    val edges_broadcast: Broadcast[Map[Long, (Long, Long)]] = sc.broadcast(edges_RDD.collect().toMap)


    start_time =System.currentTimeMillis()
    //transfer traj's edge to vertex
    val trajsByVertexs_RDD: RDD[(Long, Iterable[Long])] = Trajectory.transferToByVertex(trajsByEdges_RDD,edges_broadcast).cache()
//    val trajsByVertexs: Array[(Long, Iterable[Long])] = trajsByVertexs_RDD.collect()
    //the data is big and if you broadcast it,sc will use disk instead of memory
    end_time = System.currentTimeMillis()
    logInfo("transfer traj's edge to vertex use time:"+(end_time - start_time))
//    val trajsByVertexs_broadcast: Broadcast[Array[(Long, Iterable[Long])]] = sc.broadcast(trajsByVertexs)
    //get different start_vertex and end_vertex group

    val s_e_RDD: RDD[(Long, Long)] = trajsByVertexs_RDD.map {
      traj =>
        val vertexs = traj._2.toArray
        val start_vertex_id = vertexs(0)
        val end_vertex_id = vertexs(vertexs.length - 1)
        (start_vertex_id, end_vertex_id)
    }

//    getODPairCount
//    val odPair_count: RDD[((Long, Long), Int)] = countODPair(s_e_RDD)
//
//    odPair_count.saveAsTextFile(HDFS_BASE_RESULT_PATH+"odPairCountResult/")



//  get odpair which it's count > 100
    val odpairs: List[(Long, Long)] = getODPairListExceedNum("hdfs://node1:9000/user/lvzhongjian/result/odPairCountResult/part-00000",sc,100)
    println("count > 100 odpairs' size = "+odpairs.size)
    val odpairs_broadcast: Broadcast[List[(Long, Long)]] = sc.broadcast(odpairs)

//    start_time = System.currentTimeMillis()
//    //count each od pair's trajs which are going them
//    val odpairWithAlltrajs: RDD[((Long, Long), Array[Long])] = distinct_s_e.odpairWithAlltrajs {
//      x =>
//        val start_id = x._1
//        val end_id = x._2
//        val traj_ids = new ArrayBuffer[Long]()
//        trajsByVertexs_broadcast.value.foreach {//find traj  go through S and E vertex
//          traj =>
//            val traj_id = traj._1
//            val vertexs = traj._2.toArray
//            if (Trajectory.containsStartAndEndVertex(vertexs, start_id, end_id))
//              traj_ids += traj_id
//        }
//        ((start_id, end_id), traj_ids.toArray)
//    }
//
//    //count od pairs of each traj，it is not a good method because it cost too much time
    println("start count od pairs of each traj ")
    val odpairWithOneTraj_RDD:RDD[((Long,Long),Long)] = trajsByVertexs_RDD.flatMap{
      x =>
          val traj_id = x._1
          val vertex_idsOfTraj = x._2
          var result_list:List[((Long,Long),Long)] = Nil
          odpairs_broadcast.value.foreach{//find od pair in this traj
              s_e_id =>
                if(Trajectory.containsStartAndEndVertex(vertex_idsOfTraj.toArray,s_e_id._1,s_e_id._2)){//find od pair
                  result_list = ((s_e_id._1,s_e_id._2),traj_id)::result_list
                }
          }
          result_list
    }.cache()
//    odpairWithOneTraj_RDD.saveAsTextFile(HDFS_BASE_RESULT_PATH+"odPairWithOneTraj")

//
////    println("od_pair_traj_result size:"+odpairWithOneTraj_RDD.count())
//    println("save od_pair_traj_result")
//    val odwriter = new FileWriter(new File(od_pair_traj_result))
//    odpairWithOneTraj_RDD.collect().foreach{
//      x => odwriter.write(x._1+"|"+x._2+"\r\n")
//
//    }
//    odwriter.flush()
//    odwriter.close()


    //group by (s_id,e_id)
    logInfo("odpairWithOneTraj_RDD start group by key")
    val odpairWithAlltrajs: RDD[((Long, Long), Iterable[Long])] = odpairWithOneTraj_RDD.groupByKey()
    end_time = System.currentTimeMillis()
    logInfo("odpairWithOneTraj_RDD end group by key")
    logInfo("count each od pair's trajs which are going them use time:"+(end_time - start_time))


    //sort by traj's size and save
    odpairWithAlltrajs.map(x => (x._2.size,x)).sortByKey(false).saveAsTextFile(Parameters.HDFS_BASE_RESULT_DIR+"odpairWithAlltrajs")
    sc.stop()
  }



  /**
   * count od pair's num,and sort by desc
   * @param s_e_RDD RDD[(s_id,e_id)]
   * @return RDD[(s_id,e_id),num]
   */
  def countODPair(s_e_RDD: RDD[(Long, Long)]):RDD[((Long,Long),Int)] = {
    s_e_RDD.map(x => (x,1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
  }


  /**
   *
   * @param odPair_count_result_path  RDD.saveAsTextFilePath + /{part-XXXXX} path RDD should be RDD[((Long,Long),Int)]
   * @param sc SparkContext
   * @param num minimum count num
   * @return
   */
  def getODPairListExceedNum(odPair_count_result_path:String,sc:SparkContext,num:Int): List[(Long, Long)] = {
    val odPair_count_RDD: RDD[String] = sc.textFile(odPair_count_result_path)
    val regex = """\(\(([0-9]+),([0-9]+)\),([0-9]+)\)""".r
    var s_e_list: List[(Long, Long)] = Nil
    odPair_count_RDD.collect()foreach {
      line =>
//        println(line)
        val regex(s_id, e_id, count) = line
//        println(s_id,e_id,count)
        if (count.toInt > num) {
          s_e_list = (s_id.toLong, e_id.toLong) :: s_e_list
        }
    }
    s_e_list
  }




}