package com.ada.routecount.main

import java.io.{File, FileWriter}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, Logging, SparkConf, SparkContext}

/**
  * Created by lzj on 2016/7/10.
  */

/**
  * the main application to count route
  */
object RouteCounter extends Logging {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("RouteCounter").setMaster("local[3]")
        conf.set("spark.driver.maxResultSize", "8g")
        val sc = new SparkContext(conf)

        val traj_data_path: String = "D:\\lzj_spark_test\\small_tarjs.txt"
        val edge_data_path: String = "D:\\lzj_spark_test\\edges_new.txt"
        val route_result_path: String = "D:\\lzj_spark_test\\data\\route.txt"

//        val traj_data_path: String = "file:/home/liyang/lvzhongjian/data/trajectory_beijing_new.txt"
//        val edge_data_path: String = "file:/home/liyang/lvzhongjian/data/edges_new.txt"
//        val route_result_path: String = "/home/liyang/lvzhongjian/result/routes.txt"

        val trajUtil = new Trajectory(sc)
        //get trajs_RDD
        val trajs_RDD: RDD[(Long, Array[Long])] = trajUtil.loadTrajectoryFromDataSource(traj_data_path)
        val graph = new Graph(sc)

        //get edges_RDD
        val edges_RDD: RDD[(Long, (Long, Long))] = graph.loadEdgeFromDataSource(edge_data_path)

        val edges_broadcast: Broadcast[Map[Long, (Long, Long)]] = sc.broadcast(edges_RDD.collect().toMap)

        //transform trajs to routes
        val routes_RDD: RDD[((Long, Long), Route)] = Route.transformToRoutesFromTrajs(trajs_RDD,edges_broadcast)

        //group by (s_v_id,e_v_id)
        val routes_grouped_RDD: RDD[((Long, Long), Iterable[Route])] = routes_RDD.groupByKey()

        //get merged route
        val route_merged_RDD: RDD[((Long, Long), Iterable[Route])] = Route.mergeRoute(routes_grouped_RDD)


//        val VWrite = new FileWriter(new File(route_result_path))
//        route_merged_RDD.collect().foreach{
//            x=>VWrite.write(x.toString()+"\r\n")
//        }
//        VWrite.close()

        //get route id and set
        //Route.resetRouteId(route_merged_RDD)

        //find each route's expandroute
       val route_merged_broadcast: Broadcast[Map[(Long, Long), Iterable[Route]]] = sc.broadcast(route_merged_RDD.collect().toMap)
        route_merged_RDD.map{
            x =>
                val route_merged_and_found_list = Nil
                val start_vertex_id = x._1._1
                val end_vertex_id = x._1._2
                val routes = x._2
                val compare_routes: Map[(Long, Long), Iterable[Route]] = route_merged_broadcast.value
                //search expandroute for each route
                compare_routes.foreach{
                    x =>
                        val compare_s_v_id = x._1._1
                        val compare_e_v_id = x._1._2
                        //exist end or start vertex not same
                        if(start_vertex_id != compare_s_v_id || end_vertex_id != compare_e_v_id){
                            route_merged_and_found_list

                        }
                }
        }

    }



}

