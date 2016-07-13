package com.ada.routecount.main.util

import com.ada.routecount.main.model.Point

/**
 * Created by JQ-Cao on 2016/5/16.
 */
object Tool {
  private val base32 = Array('0' , '1', '2', '3', '4', '5', '6', '7', '8', '9',  'B', 'C',
    'D','E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U','V', 'W', 'X', 'Y', 'Z')
  private val decodeMap = base32.zipWithIndex.toMap

  private var precision = 15
  private val bits = Array(16,8,4,2,1)

  def setPrecision(precision:Int): Unit ={
    this.precision = precision
  }

  def getPrecision(x:Double,precision:Double):Double={
    val base = Math.pow(10,-precision)
    x - x%base
  }

  def encode(point: Point):String = {
    encode(point.lat,point.lng)
  }

  def encode(latitude:Double,longitude:Double):String={
    val lat_interval =Array(-90.toDouble,90.toDouble)
    val lon_interval =Array(-180.toDouble,180.toDouble)
    val geoHash = new StringBuffer()
    var is_even  = true
    var bit =0
    var ch =0
    while (geoHash.length()<precision){
      var mid = 0.0
      if(is_even){
        mid = (lon_interval(0)+lon_interval(1))/2
        if(longitude > mid){
          ch |= bits(bit)
          lon_interval(0) = mid
        }else{
          lon_interval(1) = mid
        }
      }else{
        mid = (lat_interval(0)+lat_interval(1))/2
        if(latitude>mid){
          ch |= bits(bit)
          lat_interval(0) = mid
        }else{
          lat_interval(1) = mid
        }
      }
      is_even = !is_even

      if(bit < 4){
        bit+=1
      }else{
        geoHash.append(base32(ch))
        bit = 0
        ch = 0
      }
    }
    geoHash.toString
  }

  def decode(geoHash:String):Array[Double]={
    val ge = decode_exactly(geoHash)
    val lat = ge(0)
    val lon = ge(1)
    val lat_err = ge(2)
    val lon_err = ge(3)
    val lat_precision = Math.max(1,Math.round(-Math.log10(lat_err))) -1
    val lon_precision = Math.max(1,Math.round(-Math.log10(lon_err))) -1
    Array(getPrecision(lat,lat_precision),getPrecision(lon,lon_precision))
  }

  def decode_exactly(geoHash:String):Array[Double]={
    val lat_interval =Array(-90.toDouble,90.toDouble)
    val lon_interval =Array(-180.toDouble,180.toDouble)
    var lat_err = 90.0
    var lon_err = 180.0
    var is_even = true
    for(i<-0 until geoHash.length){
      val cd = decodeMap(geoHash.charAt(i))
      for(z<-bits.indices){
        val mask = bits(z)
        if(is_even){
          lon_err/=2
          if((cd&mask)!=0){
            lon_interval(0) = (lon_interval(0)+lon_interval(1))/2
          }else{
            lon_interval(1) = (lon_interval(0)+lon_interval(1))/2
          }
        }else{
          lat_err/=2
          if((cd&mask)!=0){
            lat_interval(0) = (lat_interval(0)+lat_interval(1))/2
          }else{
            lat_interval(1) = (lat_interval(0)+lat_interval(1))/2
          }
        }
        is_even = !is_even
      }
    }
    Array((lat_interval(0)+lat_interval(1))/2,(lon_interval(0)+lon_interval(1))/2,lat_err,lon_err)
  }

  def getAround(point: Point,radius:Int):Array[Double]={
    getAround(point.lat,point.lng,radius)
  }
  def getAround(latitude:Double,longitude:Double,radius:Int):Array[Double]={

    val degree = (24901 * 1609) / 360.0
    val radiusLat = 1 / degree * radius
    val minLat = latitude - radiusLat
    val maxLat = latitude + radiusLat

    val mpdLng = degree * Math.cos(latitude * (Math.PI / 180))
    val dpmLng = 1 / mpdLng
    val radiusLng = dpmLng * radius
    val minLng = longitude - radiusLng
    val maxLng = longitude + radiusLng
    Array(minLat, minLng, maxLat, maxLng)

  }

  def initTuple_2Value(trajectory:Array[Long],edgeAboutVertexDistanceMap:Map[Long,Double]):(List[(Long,Double)],Double)={
    var edgeListTemp:List[(Long, Double)]=Nil
    var min = 900000000.0
    for(edgeTuple<-trajectory.reverse){
      if(min>edgeAboutVertexDistanceMap.getOrElse(edgeTuple,min)){
        min = edgeAboutVertexDistanceMap(edgeTuple)
      }
      edgeListTemp = (edgeTuple,min)::edgeListTemp
    }
    (edgeListTemp,min)
  }
}
