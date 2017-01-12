package org.locationtech.geomesa.spark.api.java;

import org.locationtech.geomesa.spark.GeoMesaSpark$;

import java.io.Serializable;
import java.util.Map;

public class JavaGeoMesaSpark {
    public static JavaSpatialRDDProvider apply(Map<String, Serializable> params) {
        return new JavaSpatialRDDProvider(GeoMesaSpark$.MODULE$.apply(params));
    }
}
