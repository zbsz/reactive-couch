package com.geteit.rcouch.views


/**
  */
case class View(name: String, doc: DesignDocument, reduce: Boolean = false) {
  val path = doc.path / "_view" / name
}