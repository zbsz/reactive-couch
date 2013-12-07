package com.geteit.rcouch.views

import spray.http.{HttpCharsets, HttpResponse, Uri}
import spray.json._
import com.geteit.rcouch.views.DesignDocument.ViewDef

case class DesignDocument(name: String, bucket: String, views: Map[String, ViewDef]) {

  val path = Uri.Path./(bucket) / "_design" / name

  def view(name: String) = views.get(name) map (vd => View(name, this, vd.reduce.isDefined))
}

/**
  */
object DesignDocument {

  def ddocs(bucket: String, r: HttpResponse): List[DesignDocument] =
    JsonProtocol.ddocs(bucket, r.entity.asString(HttpCharsets.`UTF-8`))

  def apply(bucket: String, r: HttpResponse): DesignDocument = {
    import JsonProtocol._

    val meta = r.headers.find(_.name == "X-Couchbase-Meta").get.value

    val JsString(id) = JsonParser(meta).asJsObject.fields("id")
    val views = JsonParser(r.entity.asString(HttpCharsets.`UTF-8`)).asJsObject
                  .fields("views").asJsObject.fields.map(p => p._1 -> p._2.convertTo[ViewDef])

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

  case class ViewDef(map: Option[MapFunction], reduce: Option[ReduceFunction])


  object JsonProtocol extends DefaultJsonProtocol {

    implicit object ReduceFunctionFormat extends RootJsonFormat[ReduceFunction] {
      def write(obj: ReduceFunction): JsValue = JsString(obj.js)
      def read(json: JsValue): ReduceFunction = ReduceFunction(json.asInstanceOf[JsString].value)
    }
    implicit object MapFunctionFormat extends RootJsonFormat[MapFunction] {
      def write(obj: MapFunction): JsValue = JsString(obj.js)
      def read(json: JsValue): MapFunction = MapFunction(json.asInstanceOf[JsString].value)
    }
    implicit val ViewDefFormat: RootJsonFormat[ViewDef] = jsonFormat2(ViewDef)

    implicit object DocumentFormat extends RootJsonFormat[DesignDocument] {
      def write(obj: DesignDocument): JsValue = JsObject(Map("views" -> obj.views.toJson))
      def read(json: JsValue): DesignDocument = ???
    }


    def ddocs(bucket: String, json: String): List[DesignDocument] = {
      val JsArray(els) = JsonParser(json).asJsObject.fields("rows")
      els.map(v => doc(bucket, v.asJsObject.fields("doc").asJsObject))
    }

    def doc(bucket: String, obj: JsObject): DesignDocument = {
      val JsString(id) = obj.fields("meta").asJsObject.fields("id")
      val views = obj.fields("json").asJsObject.fields("views").asJsObject.fields.map(p => p._1 -> p._2.convertTo[ViewDef])

      DesignDocument(id.substring("_design/".length), bucket, views)
    }
  }
}

