package com.ada.routecount.main.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by JQ-Cao on 2016/5/16.
  */
trait LoaderTrajectory {
    def loadTrajectoryFromDataSource(trajectoryFile: String): RDD[(Long, Array[Long])]
}

class Trajectory(val sc: SparkContext) extends LoaderTrajectory with Logging with Serializable {

    //从文件中读取轨迹 返回RDD[(Long//轨迹id,Array[(Long)])//采样点路段id]
    override
    def loadTrajectoryFromDataSource(trajectoryFile: String): RDD[(Long, Array[Long])] = {
        logInfo("Loading Trajectory data from %s".format(trajectoryFile))
        sc.textFile(trajectoryFile).map(x => x.split(",")).filter(x => x.length > 10).map {
            temp =>
                var arrayList: List[Long] = Nil
                var preEdgeId: Long = 0
                for (i <- 1 until temp.size) {
                    val edgeIdWithTime = temp(i).split("\\|")
                    if (preEdgeId != edgeIdWithTime(1).toLong) {
                        arrayList = edgeIdWithTime(1).toLong :: arrayList
                        preEdgeId = edgeIdWithTime(1).toLong
                    }
                }
                (temp(0).toLong, arrayList.reverse.toArray)
        }.filter(x => x._2.length > 10)
    }


}

object Trajectory {
    //  def filter_trajectory(trajectory_RDD: RDD[(Long,Array[(Long,Double)])]):RDD[(Long,Array[(Long,Double)])]={
    //    trajectory_RDD.filter()
    //  }
    //根据路段id做轨迹索引 key为路段id value为轨迹id
    def indexOfTrajectory(trajectory_RDD: RDD[(Long, Array[Long])]): RDD[(Long, Iterable[Long])] = {
        trajectory_RDD.flatMap {
            //edgeId,trajectoryId
            x => x._2.map(y => (y, x._1))
        }.groupByKey()
    }

    /**
     * tranform traj by edge_ids to traj by vertex_ids
     * @param trajectory_RDD [traj_id,Array(edge_ids)]
     * @param edges_broadcast
     * @return RDD[traj_id,Array(vertex_ids)]
     */
    def transferToVertex(trajectory_RDD:RDD[(Long,Array[Long])],
                    edges_broadcast:Broadcast[Map[Long, (Long, Long)]]):RDD[(Long,Iterable[Long])] = {
        val edges: Map[Long, (Long, Long)] = edges_broadcast.value
        trajectory_RDD.map({
            traj => val traj_id = traj._1
                    val vertex_ids = new ArrayBuffer[Long]()
                    var last_vertex:Long = 0
                    traj._2.foreach{
                        edgeid =>
                            val s_v_id = edges.get(edgeid).get
                            vertex_ids += s_v_id._1
                            last_vertex = s_v_id._2
                    }
                    vertex_ids += last_vertex
                    (traj_id,vertex_ids.toIterable)
        })
    }

    /**
     * judge traj whether contains start and end vertex(direction into consideration)
     * @param vertex_idsInTraj vertex_array in one traj
     * @param start_vertex_id
     * @param end_vertex_id
     * @return
     */
    def containsStartAndEndVertex(vertex_idsInTraj:Array[Long],start_vertex_id:Long,end_vertex_id:Long) : Boolean={
        var hasFoundStart = false
        vertex_idsInTraj.foreach{
            id =>
                if(!hasFoundStart) {
                    //not find start_vertex
                    if (id == start_vertex_id)
                        hasFoundStart = true
                }
                else {
                    //has found start_vertex,then find end_vertex
                    if (id == end_vertex_id)
                        return true
                }
        }
        false
    }
}
