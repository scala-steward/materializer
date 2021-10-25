package org.renci.materializer

import caseapp.core.Error.MalformedValue
import caseapp.core.argparser.{ArgParser, SimpleArgParser}
import org.renci.materializer.Config.{BoolValue, FalseValue, TrueValue}

final case class Config(ontologyFile: String,
                        input: String,
                        output: String,
                        suffixOutput: BoolValue = FalseValue,
                        outputGraphName: Option[String],
                        suffixGraph: BoolValue = TrueValue,
                        markDirectTypes: BoolValue = FalseValue,
                        outputIndirectTypes: BoolValue = TrueValue,
                        parallelism: Int = 16,
                        filterGraphQuery: Option[String])

object Config {

  /**
    * This works around some confusing behavior in case-app boolean parsing
    */
  sealed trait BoolValue {

    def bool: Boolean

  }

  case object TrueValue extends BoolValue {

    def bool = true

  }

  case object FalseValue extends BoolValue {

    def bool = false

  }

  implicit val argParser: ArgParser[BoolValue] = SimpleArgParser.from[BoolValue]("boolean value") { arg =>
    arg.toLowerCase match {
      case "true"  => Right(TrueValue)
      case "false" => Right(FalseValue)
      case "1"     => Right(TrueValue)
      case "0"     => Right(FalseValue)
      case _       => Left(MalformedValue("boolean value", arg))
    }
  }

}
