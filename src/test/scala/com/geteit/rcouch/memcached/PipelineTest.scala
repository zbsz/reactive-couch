package com.geteit.rcouch.memcached

import org.scalatest.{Matchers, FeatureSpec}
import akka.io._
import org.scalatest.prop.Checkers
import org.scalacheck.Prop._
import akka.util.ByteString
import com.geteit.rcouch.memcached.BinaryPipeline.{Opcode, ResponseMagic}
import com.geteit.rcouch.memcached.Memcached.{Status, GetResponse, AuthList, Command}
import akka.actor.{ActorContext, Actor, ActorSystem}
import akka.testkit.TestActorRef
import com.geteit.rcouch.memcached.BinaryPipeline.Frame
import scala.Some

/**
  */
class PipelineTest extends FeatureSpec with Matchers with Checkers {

  feature("MemcachedFrameStage") {
    scenario("Encode/decode frame") {
      val pp = PipelineFactory.buildFunctionTriple(new PipelineContext() {}, new MemcachedFrameStage)

      def encode(f: Frame): ByteString = pp.commands(f)._2.head
      def decode(b: ByteString): Frame = pp.events(b)._1.head

      check(forAll { (op: Byte, key: Option[String], value: Option[String], extras: Option[String], status: Short, cas: Long, opaque: Int) =>
        (key != Some("") && value != Some("") && extras != Some("") && (extras == None || ByteString(extras.get).length < 256)) ==>
          {
            val f = Frame(op, key.map(ByteString(_)), value.map(ByteString(_)), extras.map(ByteString(_)), status, cas, opaque)
            val res = decode(ResponseMagic +: encode(f).tail)
//            if (res != f) info(s"$res \n!= \n$f\nargs: ${(key, value, extras)}\n\n")
            res == f
          }
      })
    }

    scenario("Encode AuthList frame") {
      val pp = PipelineFactory.buildFunctionTriple(new PipelineContext() {}, new MemcachedFrameStage)

      def encode(f: Frame): ByteString = pp.commands(f)._2.head
      def decode(b: ByteString): Frame = pp.events(b)._1.head

      val encoded = encode(Frame(Opcode.AuthList, None, None, None, 0, 0, 1))
      info(s"AuthList: $encoded")
    }
  }

  feature("MemcachedMessageStage") {
    scenario("Encode AuthList") {
      implicit val system = ActorSystem("MyActorSystem")
      val actor = TestActorRef(new Actor { def receive = { case _ ⇒ } })
      val ctx = new HasActorContext { def getContext: ActorContext = actor.underlyingActor.context }

      val pp = PipelineFactory.buildFunctionTriple(ctx, new MemcachedMessageStage)

      def encode(c: Command) = pp.commands(c)._2.head
      def decode(f: Frame) = pp.events(f)._1.head

      val f = encode(AuthList())
      f should be(Frame(Opcode.AuthList, None, None, None, 0, 0, f.opaque))
    }

    scenario("Decode GetResponse NotFound") {
      implicit val system = ActorSystem("MyActorSystem")
      val actor = TestActorRef(new Actor { def receive = { case _ ⇒ } })
      val ctx = new HasActorContext { def getContext: ActorContext = actor.underlyingActor.context }

      val pp = PipelineFactory.buildFunctionTriple(ctx, new MemcachedMessageStage)

      def encode(c: Command) = pp.commands(c)._2.head
      def decode(f: Frame) = pp.events(f)._1.head

      val frame = Frame(Opcode.Get, None, None, None, Memcached.Status.NotFound, 0, 0)
      decode(frame) should be(GetResponse(Opcode.Get, None, ByteString(), 0, 0, Status.NotFound, 0))
    }
  }

  class EmptyActor extends Actor {
    def receive: Actor.Receive = {
      case _ =>
    }
  }

  feature("Pipeline") {
//    scenario("extract line") {
//      val str = ByteString(86, 65, 76, 85, 69, 32, 107, 101, 121, 32, 48, 32, 53, 13, 10, 118, 97, 108, 117, 101, 13, 10, 69, 78, 68, 13, 10)
//      val line = TextPipeline.extractLine(str)
//
//      line should equal(Some(ByteString(86, 65, 76, 85, 69, 32, 107, 101, 121, 32, 48, 32, 53, 13, 10)))
//    }
//
//    scenario("decode get response") {
//      val pipeline = new TextPipeline()
//      val str = ByteString(86, 65, 76, 85, 69, 32, 107, 101, 121, 32, 48, 32, 53, 13, 10, 118, 97, 108, 117, 101, 13, 10, 69, 78, 68, 13, 10)
//
//      val res = pipeline.apply(null).eventPipeline(str)
//
//      res.head should equal(Left(GetResponse("key", ByteString("value"), 0, None)))
//    }
  }
}
