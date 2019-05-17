package com.waiyan.ume.flink.api

import com.google.gson.annotations.SerializedName

case class CarPark(
  // @SerializedName alone doesn't work for case classes
  @(SerializedName @scala.annotation.meta.field)("CarParkID")
  id: String,
  @(SerializedName @scala.annotation.meta.field)("Area")
  area: String,
  @(SerializedName @scala.annotation.meta.field)("Development")
  development: String,
  @(SerializedName @scala.annotation.meta.field)("Location")
  location: String,
  @(SerializedName @scala.annotation.meta.field)("AvailableLots")
  availableLots: String,
  @(SerializedName @scala.annotation.meta.field)("LotType")
  lotType: String,
  @(SerializedName @scala.annotation.meta.field)("Agency")
  agency: String)

case class JsonCarParkResp(
  value: Array[CarPark]
)
