package com.github.edge.roman.spear.util

import org.apache.log4j.Logger
import org.stringtemplate.v4.{ST, STGroupFile, STGroupString, StringRenderer}

class StringTemplateUtil {
  val logger = Logger.getLogger(getClass.getName)

  def renderTemplate(templateGroupName: String, templateName: String, attributeName: String, inputMap: Any): String = {
    val stfile: STGroupFile = new STGroupFile(templateGroupName, '$', '$')
    stfile.registerRenderer(classOf[String], new StringRenderer)
    var temp: ST = null
    if (!(templateName.isEmpty) && templateName != null) {
      temp = stfile.getInstanceOf(templateName)
    }
    else {
      logger.error("Template name provided is empty")
    }
    if (temp == null) {
      throw new NullPointerException("Template " + templateName + " is not defined in " + stfile.getFileName)
    }
    temp.add(attributeName, inputMap)
    val retval: String = temp.render
    retval
  }

  /**
   * This method returns the string after rendering the template
   *
   * @param templateGroupName To which group the template belongs to
   * @param templateName      The name that is to be used in the template
   * @param templateInputs    The data to be sent for the required template name
   * @return returns string after rendering the template
   */
  def renderTemplate(templateGroupName: String, templateName: String, templateInputs: Map[String, AnyRef]): String = {
    val stfile: STGroupFile = new STGroupFile(templateGroupName, '$', '$')
    stfile.registerRenderer(classOf[String], new StringRenderer)
    var temp: ST = null
    if (!(templateName.isEmpty) && templateName != null) {
      temp = stfile.getInstanceOf(templateName)
    }
    else {
      logger.error("Template name provided is empty")
    }
    println("Aaaaaaaaaaaaaa")
    if (temp == null) {
      throw new NullPointerException("Template " + templateName + " is not defined in " + stfile.getFileName)
    }
    for (attributeName <- templateInputs.keySet) {
      temp.add(attributeName, templateInputs.get(attributeName))
    }
    val retval: String = temp.render
    return retval
  }

  def renderDynamicTemplate(templateName: String, templateInputs: Map[String, AnyRef], attribute: String, rendereddynamicTemplate: String): String = {
    val dynamicTemplate: String = "UtilConfig.GROUP_NAME" + "\n" + rendereddynamicTemplate
    System.out.println("dynamicTemplate : " + dynamicTemplate)
    val dynamicGroupTemplate: STGroupString = new STGroupString("UtilConfig.DYNAMIC_TEMPLATE", dynamicTemplate, '$', '$')
    //        dynamicGroupTemplate.registerRenderer(String.class, new StringRenderer());
    var template: ST = null
    System.out.println("templateName : " + templateName)
    System.out.println("!templateName.isEmpty() : " + !(templateName.isEmpty))
    System.out.println("dynamicGroupTemplate.getInstanceOf(templateName) : " + dynamicGroupTemplate.getInstanceOf(templateName))
    if (!(templateName.isEmpty) && templateName != null) {
      template = dynamicGroupTemplate.getInstanceOf(templateName)
      System.out.println("template : " + template)
    }
    else {
      logger.error("Template name provided is empty")
    }
    if (template == null) {
      throw new NullPointerException("Template " + templateName + " is not defined in " + dynamicGroupTemplate.getFileName)
    }
    System.out.println("templateInputs : " + templateInputs)
    template.add(attribute, templateInputs)
    template.render
  }
}
