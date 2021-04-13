package bda.spark.framework

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class BiGraph[VD : ClassTag](val graph: Graph[VD, Double]) {




}

