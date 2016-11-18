/**
 * Created by lzj on 2016/7/14.
 */
package com.ada.routecount.main


object Parameters{
  /**
   * hdfs node front part of url
   */
  val HDFS_NODE_FRONT_PART = "hdfs://node1:9000"
  /**
   * traj data path in hdfs
   */
  val traj_data_path: String = HDFS_NODE_FRONT_PART+"/user/caojiaqing/JqCao/data/trajectory_beijing_new.txt"
  /**
   * edge data path in hdfs
   */
  val edge_data_path: String = HDFS_NODE_FRONT_PART+"/user/lvzhongjian/data/edges_new.txt"

  /**
   * vertex data path in hdfs
   */
  val vertex_data_path:String = HDFS_NODE_FRONT_PART + "/user/caojiaqing/JqCao/data/vertices_new.txt"

  /**
   * base result dir to save in hdfs
   */
  val HDFS_BASE_RESULT_DIR = "/user/lvzhongjian/result/"


  /**
   * index for zhujie's trajs
   */
  val INDEX_VERTEX_TRAJ_DIR="/home/liyang/lvzhongjian/result/index_vertex_traj_zhujie"
}
