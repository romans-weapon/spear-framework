package com.github.edge.roman.spear.util

import java.util

class MappingResolverUtil {

  def parseMappingJSON(jsonInput: String): util.ArrayList[util.HashMap[String, Object]] = {
    val dataTypeMappingList = new util.ArrayList[util.HashMap[String, Object]]()
    jsonInput.split(",").foreach(value => {
      print(value)
      val datatypeMapping = new util.HashMap[String, Object]()
      value.split("->").foreach(data => {
        print(data(0))
//        datatypeMapping.put("source", data(0).toString)
//        datatypeMapping.put("target_type", data(1).toString)
//        datatypeMapping.put("target", data(2).toString)
//        dataTypeMappingList.add(datatypeMapping)
      })
    })
   // print(dataTypeMappingList)
    dataTypeMappingList
  }
}
