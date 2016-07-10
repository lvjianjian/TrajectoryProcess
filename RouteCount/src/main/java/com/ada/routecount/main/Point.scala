package com.ada.routecount.main

/**
 * Created by JQ-Cao on 2016/3/9.
 */

class Point(val lng: Double, val lat: Double) extends Serializable{
  def equals(point: Point): Boolean = lng == point.lng && lat == point.lat

  //Array(minLat, minLng, maxLat, maxLng)
  def includeOfRegion(region:Array[Double]):Boolean = lng>=region(1)&&lng<=region(3)&&lat>=region(0)&&lat<=region(2)
}

object Point {
  def apply(x: Float, y: Float): Point = {
    new Point(x.toDouble, y.toDouble)
  }

  def apply(x: Double, y: Double): Point = {
    new Point(x, y)
  }

  /*google map cal distance of two point
    var lat = [this.lat(), latlng.lat()]
    var lng = [this.lng(), latlng.lng()]
    var R = 6378137;
    var dLat = (lat[1] - lat[0]) * Math.PI / 180;
    var dLng = (lng[1] - lng[0]) * Math.PI / 180;
    var a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(lat[0] * Math.PI / 180) * Math.cos(lat[1] * Math.PI / 180) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    var d = R * c;
    return Math.round(d);
   */

//  def rad(d: Double): Double = {
//    d * math.Pi / 180.0
//  }

//  def GetPreciseDistance(pointA: Point, pointB: Point): Double = {
//    (2 * math.asin(math.sqrt(math.pow(math.sin((this.rad(pointA.lat) - this.rad(pointB.lat)) / 2), 2) +
//      math.cos(this.rad(pointA.lat)) * math.cos(this.rad(pointB.lat)) * math.pow(math.sin((this.rad(pointA.lng) -
//        this.rad(pointB.lng)) / 2), 2))) * 6370693.5 * 10000).toInt / 10000
//  }

  /*
  calculate two gpsPoint distance
   */
  def GetPreciseDistance(pointA: Point, pointB: Point): Double = {
    val dLat = (pointA.lat - pointB.lat) * math.Pi / 180
    val dLng = (pointA.lng - pointB.lng) * math.Pi / 180
    val R = 6378137
    val a = math.sin(dLat / 2) * math.sin(dLat / 2) +
      math.cos(pointB.lat * math.Pi / 180) * math.cos(pointA.lat * math.Pi / 180) * math.sin(dLng / 2) * math.sin(dLng / 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    R * c
  }

//  def GetShortDistance(pointA: Point, pointB: Point):Double={
//    // 角度转换为弧度
//    val ew1 = pointA.lng * math.Pi / 180
//    val ns1 = pointA.lat * math.Pi / 180
//    val ew2 = pointB.lng * math.Pi / 180
//    val ns2 = pointB.lat * math.Pi / 180
//    // 经度差
//    var dew = ew1 - ew2
//    // 若跨东经和西经180 度，进行调整
//    if (dew > math.Pi)
//      dew = math.Pi * 2 - dew
//    else if (dew < -math.Pi)
//      dew = 2 * math.Pi + dew
//    val dx = 6370693.5 * Math.cos(ns1) * dew // 东西方向长度(在纬度圈上的投影长度)
//    val dy = 6370693.5 * (ns1 - ns2) // 南北方向长度(在经度圈上的投影长度)
//    // 勾股定理求斜边长
//    Math.sqrt(dx * dx + dy * dy)
//
//  }
//
//  def GetDistance2(point1: Point,point2: Point):Double={
//    val lat_a = point1.lat
//    val lat_b = point2.lat
//    val lng_a = point1.lng
//    val lng_b = point2.lng
//
//    val pk:Double = 180 / 3.14169
//    val a1:Double = lat_a / pk
//    val a2 = lng_a / pk
//    val b1 = lat_b / pk
//    val b2 = lng_b / pk
//    val t1 = (math.cos(a1)) * (math.cos(a2)) * (math.cos(b1)) * (math.cos(b2))
//    val t2 = (math.cos(a1)) * (math.sin(a2)) * (math.cos(b1)) * (math.sin(b2))
//    val t3 = (math.sin(a1)) * (math.sin(b1))
//    val tt = math.acos((t1 + t2 + t3))
//    6366000 * tt
//  }
//每一纬度对应的米数
//  private val M_PER_LAT:Int = 111111
//  private val M_PER_LAT_KM = 70
//  //每一经度度对应的米数
//  private val M_PER_LNG = 70000
//  private val M_PER_LNG_KM = 111.111

//  def GetDistance(point: Point,edge:Edge):Double={
//    val cross:Double = (edge.end.point.lat - edge.start.point.lat)*M_PER_LAT_KM * (point.lat - edge.start.point.lat)*M_PER_LAT_KM +
//      (edge.end.point.lng - edge.start.point.lng)*M_PER_LNG_KM * (point.lng - edge.start.point.lng)*M_PER_LNG_KM
//    if(cross<=0)
//      return  math.sqrt(math.pow((point.lat-edge.start.point.lat)*M_PER_LAT_KM,2) + math.pow((point.lng-edge.start.point.lng)*M_PER_LNG_KM,2))
//    val d2:Double= math.pow((edge.end.point.lat - edge.start.point.lat)*M_PER_LAT_KM,2) +
//      math.pow((edge.end.point.lng - edge.start.point.lng)*M_PER_LNG_KM,2)
//    if(cross>=d2){
//      return math.sqrt(math.pow((point.lat-edge.end.point.lat)*M_PER_LAT_KM,2)+math.pow((point.lng-edge.end.point.lng)*M_PER_LNG_KM,2))
//    }
//    val r:Double = cross/d2
//    val px = edge.start.point.lat + (edge.end.point.lat - edge.start.point.lat) * r
//    val  py = edge.start.point.lng + (edge.end.point.lng - edge.start.point.lng) * r
//    math.sqrt(math.pow((point.lat-px)*M_PER_LAT_KM,2) + math.pow((py-edge.start.point.lng)*M_PER_LNG_KM,2))
//  }

  /*
  get distance between point and edge
  return Meter
   */
  def GetDistance(point: Point,edge: Edge):Double={
    val a = GetPreciseDistance(point,edge.start.point)
    val b = GetPreciseDistance(point,edge.end.point)
    if(a<10||b<10){
      return 0
    }
    val c = GetPreciseDistance(edge.start.point,edge.end.point)
    if(c<10){
      return a;
    }

    if(a*a>=b*b+c*c)
      return b;
    if(b*b>=a*a+c*c)
      return a;

    //图1
    val l=(a+b+c)/2;     //周长的一半
    val s=math.sqrt(l*(l-a)*(l-b)*(l-c));  //海伦公式求面积
    2*s/c;
  }

  def GetDistance(point: Point,start:Vertex,end:Vertex):Double={
    val a = GetPreciseDistance(point,start.point)
    val b = GetPreciseDistance(point,end.point)
    if(a<10||b<10){
      return 0
    }
    val c = GetPreciseDistance(start.point,end.point)
    if(c<10){
      return a;
    }

    if(a*a>=b*b+c*c)
      return b;
    if(b*b>=a*a+c*c)
      return a;

    //图1
    val l=(a+b+c)/2;     //周长的一半
    val s=math.sqrt(l*(l-a)*(l-b)*(l-c));  //海伦公式求面积
    2*s/c;
  }

}
