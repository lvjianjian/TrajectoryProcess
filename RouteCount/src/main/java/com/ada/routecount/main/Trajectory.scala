package com.ada.routecount.main

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

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
}
