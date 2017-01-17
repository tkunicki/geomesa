/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark

import java.io.{BufferedWriter, StringWriter}
import java.util.ServiceLoader

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.Query
import org.geotools.geojson.feature.FeatureJSON
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.reflect._

trait SpatialRDDProvider {
  import TypeDefault._

  type GeoJSONString = String

  def toScalaMap(in: SpatialRDD): RDD[Map[String, AnyRef]] = {
    val keys = in.schema.getAttributeDescriptors.map(_.getName).map(_.getLocalPart)
    in.rdd.map(sf => keys.zip(sf.getAttributes).toMap).map(m => m + ("geom" -> m("geom").toString))
  }

  def toJavaMap(in: SpatialRDD): RDD[java.util.Map[String, AnyRef]] = {
    toScalaMap(in).map(mapAsJavaMap)
  }

  def toGeoJSONString(in: SpatialRDD): RDD[GeoJSONString] = {
    in.rdd.mapPartitions(features => {
      val json = new FeatureJSON
      val sw = new StringWriter
      val bw = new BufferedWriter(sw)
      features.map(f => try { json.writeFeature(f, bw); sw.toString } finally { sw.getBuffer.setLength(0) })
    })
  }

  def transformFunc[A : ClassTag]: (SpatialRDD => RDD[A]) = (classTag[A] match {
    case ct if ct == classTag[GeoJSONString] => toGeoJSONString _
    case ct if ct == classTag[Map[String, AnyRef]] => toScalaMap _
    case ct if ct == classTag[java.util.Map[String, AnyRef]] => toJavaMap _
    case ct if ct == classTag[SimpleFeature] => in: SpatialRDD => in.rdd
    case _ => new IllegalArgumentException
  }).asInstanceOf[SpatialRDD => RDD[A]]

  def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean

  def rdd[T : ClassTag](conf: Configuration,
             sc: SparkContext,
             params: Map[String, String],
             query: Query)(implicit default: T := SimpleFeature): RDD[T] = {
      val tf = transformFunc[T]
      tf(read(conf, sc, params, query))
  }

  case class SpatialRDD(rdd: RDD[SimpleFeature], schema: SimpleFeatureType)

  protected def read(conf: Configuration,
                     sc: SparkContext,
                     params: Map[String, String],
                     query: Query) : SpatialRDD

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param params
    * @param typeName
    */
  def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit

}

object GeoMesaSpark {

  import scala.collection.JavaConversions._

  lazy val providers: ServiceLoader[SpatialRDDProvider] = ServiceLoader.load(classOf[SpatialRDDProvider])

  def apply(params: java.util.Map[String, java.io.Serializable]): SpatialRDDProvider =
    providers.find(_.canProcess(params)).getOrElse(throw new RuntimeException("Could not find a SparkGISProvider"))
}

// Resolve issue with wrapped instance of org.apache.spark.sql.execution.datasources.CaseInsensitiveMap in Scala 2.10
object CaseInsensitiveMapFix {
  import scala.collection.convert.Wrappers._

  trait MapWrapperFix[A,B] {
    this: MapWrapper[A,B] =>
      override def containsKey(key: AnyRef): Boolean = try {
        get(key) != null
      } catch {
        case ex: ClassCastException => false
      }
  }

  implicit def mapAsJavaMap[A <: String, B](m: scala.collection.Map[A, B]): java.util.Map[A, B] = m match {
    case JMapWrapper(wrapped) => wrapped.asInstanceOf[java.util.Map[A, B]]
    case _ => new MapWrapper[A,B](m) with MapWrapperFix[A, B]
  }
}

object TypeDefault {

  class :=[P,D]

  trait Default_:={
    implicit def useProvided[Provided,Default] = new :=[Provided,Default]
  }

  object := extends Default_:={
    implicit def useDefault[Default] = new :=[Default,Default]
  }
}
