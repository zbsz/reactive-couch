package com.geteit.rcouch.views

import spray.http.{HttpCharsets, Uri}
import play.api.libs.json._
import spray.http.HttpResponse
import com.geteit.rcouch.views.DesignDocument.ViewDef
import play.api.libs.json.JsString

case class DesignDocument(name: String, bucket: String, views: Map[String, ViewDef]) {

  val path = Uri.Path./(bucket) / "_design" / name

  def view(name: String) = views.get(name) map (vd => View(name, this, vd.reduce.isDefined))
}

/**
  */
object DesignDocument {

  def ddocs(bucket: String, r: HttpResponse): List[DesignDocument] =
    ddocs(bucket, r.entity.asString(HttpCharsets.`UTF-8`))

  def apply(bucket: String, r: HttpResponse): DesignDocument = {

    val meta = r.headers.find(_.name == "X-Couchbase-Meta").get.value

    val JsString(id) = Json.parse(meta) \ "id"
    val views = (Json.parse(r.entity.asString(HttpCharsets.`UTF-8`)) \ "views").as[Map[String, ViewDef]]

    DesignDocument(id.substring("_design/".length), bucket, views)
  }

  sealed trait JsFunction {
    val js: String
  }
  case class MapFunction(js: String) extends JsFunction

  sealed trait ReduceFunction extends JsFunction
  object ReduceFunction {
    case object Count extends ReduceFunction {
      val js: String = "_count"
    }
    case object Sum extends ReduceFunction {
      val js: String = "_sum"
    }
    case object Stats extends ReduceFunction {
      val js: String = "_stats"
    }
    case class Custom(js: String) extends ReduceFunction

    def apply(js: String): ReduceFunction = js match {
      case "_count" => Count
      case "_sum" => Sum
      case "_stats" => Stats
      case _ => Custom(js)
    }
  }

  case class DocumentDef(views: Map[String, ViewDef])

  case class ViewDef(map: MapFunction, reduce: Option[ReduceFunction] = None)


  implicit object ReduceFunctionFormat extends Format[ReduceFunction] {
    def reads(json: JsValue): JsResult[ReduceFunction] = json match {
      case JsString(str) => JsSuccess(ReduceFunction(str))
      case _ => JsError(s"Couldn't parse Reduce function from: $json")
    }
    def writes(o: ReduceFunction): JsValue = JsString(o.js)
  }
  implicit object MapFunctionFormat extends Format[MapFunction] {
    def reads(json: JsValue): JsResult[MapFunction] = json match {
      case JsString(str) => JsSuccess(MapFunction(str))
      case _ => JsError(s"Couldn't parse Map function from: $json")
    }
    def writes(o: MapFunction): JsValue = JsString(o.js)
  }
  implicit val ViewDefFormat = Json.format[ViewDef]
  implicit val DocumentDefFormat = Json.format[DocumentDef]

  implicit object DocumentFormat extends Format[DesignDocument] {
    def writes(obj: DesignDocument): JsValue = Json.obj("views" -> obj.views)
    def reads(json: JsValue): JsResult[DesignDocument] = ???
  }

  def ddocs(bucket: String, json: String): List[DesignDocument] = {
    val JsArray(els) = Json.parse(json) \ "rows"
    els.map(v => doc(bucket, v \ "doc")).toList
  }

  def doc(bucket: String, obj: JsValue): DesignDocument = {
    val JsString(id) = obj \ "meta" \ "id"
    val views = (obj \ "json" \ "views").as[Map[String, ViewDef]]

    DesignDocument(id.substring("_design/".length), bucket, views)
  }
}

