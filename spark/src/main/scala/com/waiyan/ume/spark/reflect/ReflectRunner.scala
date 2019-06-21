package com.waiyan.ume.spark.reflect

object ReflectRunner {

  private def reflectAnimal(methodName: String) = {
    val ru = scala.reflect.runtime.universe
    val instanceMirror = ru.runtimeMirror(getClass.getClassLoader).reflect(AnimalFactory)
    val method = ru.typeOf[AnimalFactory.type].decl(ru.TermName(methodName)).asMethod
    instanceMirror.reflectMethod(method)
  }

  def main(args: Array[String]): Unit = {

    reflectAnimal("bark")()
    // won't be executed if parentheses are missing
    reflectAnimal("bark")
    reflectAnimal("mirror")("meow")

  }

}
