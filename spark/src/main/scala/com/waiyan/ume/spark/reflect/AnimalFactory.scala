package com.waiyan.ume.spark.reflect

object AnimalFactory {

  def bark(): Unit = {
    println("woof")
  }

  def mirror(word: String): Unit = {
    println(word)
  }

}
