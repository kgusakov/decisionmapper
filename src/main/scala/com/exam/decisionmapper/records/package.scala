package com.exam.decisionmapper

import argonaut._
import Argonaut._

package object records {

  case class ProfileRecord(
      column: String,
      uniqueValues: Int,
      values: List[(String, Long)]
  )

  implicit def ProfileRecordEncoder: EncodeJson[ProfileRecord] =
    EncodeJson((p: ProfileRecord) =>
      ("Column" := p.column)
        ->: ("Unique_values" := p.uniqueValues)
        ->: ("Values" := p.values.map {
          case (column, count) => Json(column -> count.asJson)
        })
        ->: jEmptyObject
    )
}
