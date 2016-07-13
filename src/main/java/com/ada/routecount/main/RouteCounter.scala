package com.ada.routecount.main

import java.io.{File, FileWriter}

import com.ada.routecount.main.model.{Route, ExpandRoute}
import com.ada.routecount.main.util.{Graph, Trajectory}
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
        val conf = new SparkConf().setAppName("RouteCounter")
        conf.set("fs.hdfs.impl.disable.cache","true");
//        conf.setMaster("local[3]")
        conf.set("spark.driver.maxResultSize", "30g")
        val sc = new SparkContext(conf)

//        val traj_data_path: String = "D:\\lzj_spark_test\\small_tarjs.txt"
//        val edge_data_path: String = "D:\\lzj_spark_test\\edges_new.txt"
//        val route_result_path: String = "D:\\lzj_spark_test\\data\\route_result.txt"
//        val route_path: String = "D:\\lzj_spark_test\\data\\route.txt"

        val traj_data_path: String = "hdfs://node1:9000/user/caojiaqing/JqCao/data/trajectory_beijing_new.txt"
        val edge_data_path: String = "hdfs://node1:9000/user/caojiaqing/JqCao/data/edges_new.txt"
        val route_result_path: String = "/home/liyang/lvzhongjian/result/route_result.txt"
        val route_path: String = "/home/liyang/lvzhongjian/result/routes.txt"
        val route_frequency_path = "/home/liyang/lvzhongjian/result/route_frequency.txt"

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

        //get route id and set
        //Route.resetRouteId(route_merged_RDD)

        //find each route's expandroute
        val route_merged_broadcast: Broadcast[Map[(Long, Long), Iterable[Route]]] = sc.broadcast(route_merged_RDD.collect().toMap)
//        val compare_routes: Map[(Long, Long), Iterable[Route]] = route_merged_RDD.collect().toMap
        val routeWithExpand_merged_RDD: RDD[((Long, Long), Iterable[Route])] = route_merged_RDD.map {
            x =>
                var route_merged_and_found_list: List[Route] = x._2.toList
                val start_vertex_id = x._1._1
                val end_vertex_id = x._1._2
                val compare_routes: Map[(Long, Long), Iterable[Route]] = route_merged_broadcast.value
                //search expandroute for each route
                compare_routes.foreach {
                    x =>
                        val compare_s_v_id = x._1._1
                        val compare_e_v_id = x._1._2
                        val routes: Iterable[Route] = x._2
                        //exist end or start vertex not same,
                        // it means if start and end vertex are same ,it can't be expand route
                        if (start_vertex_id != compare_s_v_id || end_vertex_id != compare_e_v_id) {
                            //first filter all routes which contains v and e vertex
                            routes.filter(route => route.containsStartAndEndVertex(start_vertex_id, end_vertex_id)).
                              foreach {
                                //for each route which has s and e vertex
                                filtered_route => var hasSubRoute = false
                                    route_merged_and_found_list.foreach {
                                        route_merged_and_found =>
                                            if (!hasSubRoute) {
                                                //not find sub route,search
                                                if (filtered_route.containsSubRoute(route_merged_and_found.edge_ids)) {
                                                    hasSubRoute = true
                                                    route_merged_and_found.addExpandRoute(ExpandRoute(filtered_route.start_vertex_id, filtered_route.end_vertex_id, filtered_route.getRouteId()))
                                                }
                                            }

                                    }
                                    if (!hasSubRoute) {
                                        //if no sub route ,then create a new sub route and add to list
                                        val start_index = filtered_route.vertex_ids.indexOf(start_vertex_id)
                                        val end_index = filtered_route.vertex_ids.indexOf(end_vertex_id)
                                        val sub_vertex_ids = for (i <- 0 until (filtered_route.vertex_ids.length) if (i >= start_index && i <= end_index)) yield filtered_route.vertex_ids(i)
                                        val sub_edge_ids = for (i <- 0 until (filtered_route.edge_ids.length) if (i >= start_index && i < end_index)) yield filtered_route.edge_ids(i)
                                        val new_route = Route(sub_edge_ids.toArray, start_vertex_id, end_vertex_id, sub_vertex_ids.toArray)
                                        new_route.setFrequencyToZero()
                                        new_route.addExpandRoute(ExpandRoute(filtered_route.start_vertex_id, filtered_route.end_vertex_id, filtered_route.getRouteId()))
                                        route_merged_and_found_list = new_route :: route_merged_and_found_list
                                    }
                            }
                        }
                }
                (x._1, route_merged_and_found_list.toIterable)
        }//end find each route's expandroute


        //save result
        val VWrite = new FileWriter(new File(route_result_path))
        val routeWrite = new FileWriter(new File(route_path))
        val frequency_writer = new FileWriter(new File(route_frequency_path))
        routeWithExpand_merged_RDD.collect().foreach{
            x =>
                VWrite.write(x._1+"|")
                x._2.foreach{
                    route =>
                            //write frequency info
                            VWrite.write(route.getRouteId() + ","+route.getRouteFrequency()+"[")
                            route.getExpandRoutes().foreach{
                                er => VWrite.write(er.toString)
                            }
                            VWrite.write("]|")
                            //write edges
//                            routeWrite.write(route.getRouteId()+"|")
//                            route.edge_ids.foreach{
//                                edgeid=>routeWrite.write(edgeid+"|")
//                            }
//                            routeWrite.write("\r\n")
                }
                VWrite.write("\r\n")
                //count frequency
                frequency_writer.write(x._1+"|")
                frequency_writer.write(x._2.size)
                frequency_writer.write("\r\n")
                VWrite.flush()
                frequency_writer.flush()
        }//end write
        VWrite.flush()
        routeWrite.flush()
        VWrite.close()
        routeWrite.close()
        frequency_writer.flush()
        frequency_writer.close()

    }//end main function
}

