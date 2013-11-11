package com.geteit.rcouch.views

import spray.http.Uri

/**
  */
case class View(name: String, bucket: String, designDoc: String, map: Boolean = true, reduce: Boolean = false) {
  val path = Uri.Path(s"/$bucket/_design/$designDoc/_view/$name")
}
