package org.locationtech.geomesa.spark.api.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.geotools.data.Query;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.locationtech.geomesa.spark.TypeDefault;
import org.opengis.feature.simple.SimpleFeature;

import java.io.Serializable;
import java.util.Map;

public class JavaSpatialRDDProvider {

    private final SpatialRDDProvider wrapped;

    public JavaSpatialRDDProvider(SpatialRDDProvider wrapped) {
        this.wrapped = wrapped;
    }

    public boolean canProcess(Map<String, Serializable> params) {
        return wrapped.canProcess(params);
    }

    public JavaRDD<SimpleFeature> rdd(Configuration conf, JavaSparkContext jsc, Map<String, String> params, Query query) {
        return rdd(conf, jsc, params, query, SimpleFeature.class);
    }

    public <T> JavaRDD<T> rdd(Configuration conf, JavaSparkContext jsc, Map<String, String> params, Query query, Class<T> clazz) {
        return wrapped.rdd(conf,
                jsc.sc(),
                toScalaMap(params),
                query,
                scala.reflect.ClassTag$.MODULE$.apply(clazz),
                new TypeDefault.$colon$eq<T, SimpleFeature>()).toJavaRDD();
    }

    public void save(JavaRDD<SimpleFeature> jrdd, Map<String, String> params, String typeName) {
        wrapped.save(jrdd.rdd(), toScalaMap(params), typeName);
    }

    static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> map) {
        return scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala().toMap(scala.Predef.conforms());
    }
}
