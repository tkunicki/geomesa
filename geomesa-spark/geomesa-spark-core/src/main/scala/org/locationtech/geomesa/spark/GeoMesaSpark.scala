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

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.geotools.data.Query
import org.geotools.geojson.feature.FeatureJSON
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

trait SpatialRDDProvider extends LazyLogging {
  import org.locationtech.geomesa.spark.SpatialRDDProvider._

  def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean

  def rdd[T](conf: Configuration, sc: SparkContext, params: Map[String, String], query: Query)
                       (implicit format: Format[T]): RDD[T] = {
    format.transform(read(conf, sc, params, query))
  }

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

object SpatialRDDProvider extends LazyLogging {

  case class SpatialRDD(rdd: RDD[SimpleFeature], schema: SimpleFeatureType)

  def toValueSeq(in: SpatialRDD): RDD[Seq[AnyRef]] =
    in.rdd.map(_.getAttributes)

  def toKeyValueSeq(in: SpatialRDD): RDD[Seq[(String, AnyRef)]] = {
    val keys = in.schema.getAttributeDescriptors.map(_.getName).map(_.getLocalPart)
    toValueSeq(in).map(l => keys.zip(l))
  }

  def toScalaMap(in: SpatialRDD): RDD[Map[String, AnyRef]] = {
    val keys = in.schema.getAttributeDescriptors.map(_.getName).map(_.getLocalPart)
    in.rdd.map(sf => keys.zip(sf.getAttributes).toMap)
  }

  def toJavaMap(in: SpatialRDD): RDD[java.util.Map[String, AnyRef]] = {
    toScalaMap(in).map(mapAsJavaMap)
  }

  def toGeoJSONString(in: SpatialRDD): RDD[String] = {
    in.rdd.mapPartitions(features => {
      val json = new FeatureJSON
      val sw = new StringWriter
      val bw = new BufferedWriter(sw)
      features.map(f => try {
        json.writeFeature(f, bw); sw.toString
      } finally {
        sw.getBuffer.setLength(0)
      })
    })
  }

  def toSimpleFeature(in: SpatialRDD): RDD[SimpleFeature] = {
    val c = in.rdd.context.getConf
    if (c.getOption("spark.serializer").exists(_ == classOf[KryoSerializer].getName) &&
      c.getOption("spark.kryo.registrator").exists(_ == classOf[GeoMesaSparkKryoRegistrator].getName)) {
      GeoMesaSparkKryoRegistrator.register(in.schema)
      GeoMesaSparkKryoRegistrator.broadcast(in.rdd)
    } else {
      logger.warn(s"Unable to register SimpleFeatureType for ${in.schema.getTypeName}, kryo serializer not configured.")
    }
    in.rdd
  }

  trait Format[T] {
    def transform(in: SpatialRDD): RDD[T]
  }

  implicit object SimpleFeatureFormat extends Format[SimpleFeature] {
    def transform(in: SpatialRDD) = toSimpleFeature(in)
  }

  object NoOpFormat extends Format[SimpleFeature] {
    def transform(in: SpatialRDD) = in.rdd
  }

  implicit object ScalaMapFormat extends Format[Map[String, AnyRef]] {
    def transform(in: SpatialRDD) = toScalaMap(in)
  }

  implicit object JavaMapFormat extends Format[java.util.Map[String, AnyRef]] {
    def transform(in: SpatialRDD) = toJavaMap(in)
  }

  implicit object GeoJSONStringFormat extends Format[String] {
    def transform(in: SpatialRDD) = toGeoJSONString(in)
  }
}