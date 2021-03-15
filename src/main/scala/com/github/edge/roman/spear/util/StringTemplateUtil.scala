package com.github.edge.roman.spear.util

import org.stringtemplate.v4.{ST, STGroupFile, STGroupString, StringRenderer}

import scala.collection.immutable.HashMap

class StringTemplateUtil {

  def renderTemplate(templateGroupName: String, templateName: String, attributeName: String, input: AnyRef): String = {
    val stfile: STGroupFile = new STGroupFile(templateGroupName, '$', '$')
    stfile.registerRenderer(classOf[String], new StringRenderer)
    var temp: ST = null
    if (!(templateName.isEmpty) && templateName != null) {
      temp = stfile.getInstanceOf(templateName)
    }
    else {
     print("")
    }
    if (temp == null) {
      throw new NullPointerException("Template " + templateName + " is not defined in " + stfile.getFileName)
    }
    temp.add(attributeName, input)
    val retval: String = temp.render
    retval
  }

  def renderTemplate(templateGroupName: String, templateName: String, templateInputs: HashMap[String, AnyRef]): String = {
    val stfile: STGroupFile = new STGroupFile(templateGroupName, '$', '$')
    stfile.registerRenderer(classOf[String], new StringRenderer)
    var temp: ST = null
    if (!(templateName.isEmpty) && templateName != null) {
      temp = stfile.getInstanceOf(templateName)
    }
    else {
      //logger.error("Template name provided is empty")
    }
    if (temp == null) {
      throw new NullPointerException("Template " + templateName + " is not defined in " + stfile.getFileName)
    }
    for (attributeName <- templateInputs.keySet) {
      temp.add(attributeName, templateInputs(attributeName))
    }
    val retval: String = temp.render
    retval
  }
}
