package com.geteit.rcouch.memcached

import org.scalatest.FeatureSpec
import javax.security.sasl.Sasl
import javax.security.auth.callback.{PasswordCallback, NameCallback, Callback, CallbackHandler}

/**
  */
class SaslSpec extends FeatureSpec {

  feature("Sasl") {
    scenario("Generate PLAIN auth request") {
      val sc = Sasl.createSaslClient(Array("PLAIN"), null, "memcached", "localhost", null, new CallbackHandler {
        def handle(callbacks: Array[Callback]): Unit = {
          callbacks foreach {
            case c: NameCallback =>
              info("name callback")
              c.setName("name")
            case p: PasswordCallback =>
              info("password callback")
              p.setPassword("passwd".toCharArray)
            case c => info("Unsupported callback: $c")
          }
        }
      })

      info(s"hasInitialResponse: ${sc.hasInitialResponse}")
      info(s"initial response: ${sc.evaluateChallenge(Array()).mkString(",")}")
    }
  }

}
