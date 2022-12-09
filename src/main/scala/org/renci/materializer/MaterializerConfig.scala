package org.renci.materializer

import caseapp.core.Error.MalformedValue
import caseapp.core.argparser.{ArgParser, SimpleArgParser}
import org.renci.materializer.MaterializerConfig.{Arachne, BoolValue, FalseValue, Reasoner, TrueValue}
import org.semanticweb.owlapi.functional.parser.OWLFunctionalSyntaxOWLParserFactory
import org.semanticweb.owlapi.io.OWLParserFactory
import org.semanticweb.owlapi.manchestersyntax.parser.ManchesterOWLSyntaxOntologyParserFactory
import org.semanticweb.owlapi.model.OWLOntology
import org.semanticweb.owlapi.oboformat.OBOFormatOWLAPIParserFactory
import org.semanticweb.owlapi.owlxml.parser.OWLXMLParserFactory
import org.semanticweb.owlapi.rdf.rdfxml.parser.RDFXMLParserFactory
import org.semanticweb.owlapi.rdf.turtle.parser.TurtleOntologyParserFactory
import org.semanticweb.owlapi.rio.{RioRDFXMLParserFactory, RioTurtleParserFactory}

sealed trait MaterializerConfig

object MaterializerConfig {

  final case class File(ontologyFile: String,
                        ontologyFormat: Option[OWLParserFactory],
                        input: String,
                        output: String,
                        suffixOutput: BoolValue = FalseValue,
                        outputGraphName: Option[String],
                        suffixGraph: BoolValue = TrueValue,
                        reasoner: Reasoner = Arachne,
                        markDirectTypes: BoolValue = FalseValue,
                        outputIndirectTypes: BoolValue = TrueValue,
                        outputInconsistent: BoolValue = FalseValue,
                        parallelism: Int = 16,
                        filterGraphQuery: Option[String]) extends MaterializerConfig

  final case class Server(ontologyFile: String,
                          ontologyFormat: Option[OWLParserFactory],
                          reasoner: Reasoner = Arachne) extends MaterializerConfig

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

  sealed trait Reasoner {

    def construct(ontology: OWLOntology): Materializer

  }

  case object Arachne extends Reasoner {

    def construct(ontology: OWLOntology): Materializer =
      ArachneMaterializer.apply(ontology)
  }

  case object Whelk extends Reasoner {

    def construct(ontology: OWLOntology): Materializer =
      WhelkMaterializer.apply(ontology)

  }

  implicit val reasonerParser: ArgParser[Reasoner] = SimpleArgParser.from[Reasoner]("reasoner") { arg =>
    arg.toLowerCase match {
      case "arachne" => Right(Arachne)
      case "whelk"   => Right(Whelk)
      case _         => Left(MalformedValue("reasoner name", arg))
    }
  }

  implicit val formatParser: ArgParser[OWLParserFactory] = SimpleArgParser.from[OWLParserFactory]("format") { arg =>
    arg.toLowerCase match {
      case "ofn" => Right(new OWLFunctionalSyntaxOWLParserFactory())
      case "owl" => Right(new RioRDFXMLParserFactory())
      case "ttl" => Right(new RioTurtleParserFactory())
      case "omn" => Right(new ManchesterOWLSyntaxOntologyParserFactory())
      case "owx" => Right(new OWLXMLParserFactory())
      case "obo" => Right(new OBOFormatOWLAPIParserFactory())
    }
  }

}
