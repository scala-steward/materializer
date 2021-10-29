package org.renci.materializer

import caseapp._
import org.apache.commons.io.FileUtils
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.query.{Query, QueryExecutionFactory, QueryFactory}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.system.{StreamRDF, StreamRDFWriter}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.jena.sparql.core.Quad
import org.apache.jena.vocabulary.{OWL2, RDF}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._
import zio._
import zio.blocking.{Blocking, effectBlocking}
import zio.stream._

import java.io.{File, FileOutputStream, OutputStream}
import java.lang.System.currentTimeMillis
import scala.jdk.CollectionConverters._
import scala.util.Using

object Main extends ZCaseApp[Config] {

  private val ProvDerivedFrom = NodeFactory.createURI("http://www.w3.org/ns/prov#wasDerivedFrom")

  override def run(config: Config, arg: RemainingArgs): URIO[ZEnv, ExitCode] = {
    val managedWriter = Managed.fromAutoCloseable(ZIO.effect(new FileOutputStream(new File(config.output))))
      .flatMap(createStreamRDF)
    val program = managedWriter.use { quadsWriter =>
      for {
        ontology <- loadOntology(config.ontologyFile)
        filterGraphQueryOpt <- ZIO.foreach(config.filterGraphQuery)(loadFilterGraphQuery)
        reasoner = config.reasoner.construct(ontology, config.markDirectTypes.bool, config.outputIndirectTypes.bool)
        _ <- ZIO.succeed(scribe.info("Prepared reasoner"))
        start <- ZIO.succeed(currentTimeMillis())
        inferencesStream = streamModels(config.input, config.parallelism)
          .filter(model => filterGraphQueryOpt.forall(q => shouldUseGraph(q, model.model)))
          .mapMParUnordered(config.parallelism)(computeInferences(_, reasoner, config))
          .tap {
            case _: Inferences      => ZIO.unit
            case Inconsistent(path) => ZIO.succeed(scribe.warn(s"Inconsistent dataset: $path"))
          }
          .collect { case inf: Inferences => inf }
        _ <- inferencesStream.foreach { case Inferences(_, graphOpt, triples) =>
          writeModel(graphOpt, triples, quadsWriter)
        }
        stop <- ZIO.succeed(currentTimeMillis())
        _ <- ZIO.succeed(scribe.info(s"Reasoning done in: ${(stop - start) / 1000.0}s"))
      }
      yield ()
    }
    program.tapError(e => ZIO.succeed(e.printStackTrace())).exitCode
  }

  def computeInferences(model: ModelFromPath, materializer: Materializer, config: Config): Task[Result] = {
    for {
      modelIRI <- ZIO.fromOption(findModelIRI(model.model))
        .orElseFail(new Exception(s"Dataset with no ontology IRI in file ${model.path}"))
      graphOpt = determineOutGraph(modelIRI, config).map(NodeFactory.createURI)
      provenanceOpt = graphOpt.map(g => Triple.create(g, ProvDerivedFrom, NodeFactory.createURI(modelIRI)))
      inferred = materializer.materialize(model.model, config.outputInconsistent.bool)
    } yield {
      inferred match {
        case Some(inferences) => Inferences(model.path, graphOpt, inferences ++ provenanceOpt)
        case None             => Inconsistent(model.path)
      }
    }
  }

  def writeModel(graphOpt: Option[Node], triples: Set[Triple], quadsWriter: StreamRDF): RIO[Blocking, Unit] =
    effectBlocking {
      graphOpt match {
        case Some(graph) => triples.foreach(t => quadsWriter.quad(Quad.create(graph, t)))
        case None        => triples.foreach(quadsWriter.triple)
      }
    }

  def loadOntology(path: String): RIO[Blocking, OWLOntology] = for {
    manager <- ZIO.effect(OWLManager.createOWLOntologyManager())
    ontology <- effectBlocking(manager.loadOntology(IRI.create(new File(path))))
    _ <- ZIO.succeed(scribe.info("Loaded ontology"))
  } yield ontology

  def streamModels(path: String, parallelism: Int): ZStream[Blocking, Throwable, ModelFromPath] = {
    val fileOrDirectory = new File(path)
    val filesZ = for {
      isDirectory <- ZIO.effect(fileOrDirectory.isDirectory)
      inputFiles <- if (isDirectory)
        effectBlocking(FileUtils.listFiles(fileOrDirectory, null, true).asScala.to(List))
      else ZIO.succeed(List(fileOrDirectory))
    } yield inputFiles
    ZStream.fromIterableM(filesZ).flatMapPar(parallelism)(file => loadModelsFromFile(file.getPath))
  }

  def loadModelsFromFile(filePath: String): ZStream[Blocking, Throwable, ModelFromPath] = {
    val models = effectBlocking(RDFDataMgr.loadDataset(filePath)).map {
      dataset =>
        val defaultModel = ModelFromPath(dataset.getDefaultModel, filePath)
        val namedModels = dataset.listNames().asScala.map(graphName => ModelFromPath(dataset.getNamedModel(graphName), filePath)).to(List)
        defaultModel :: namedModels
    }
    ZStream.fromIterableM(models)
  }

  def createStreamRDF(output: OutputStream): TaskManaged[StreamRDF] = {
    Managed.makeEffect {
      val stream = StreamRDFWriter.getWriterStream(output, RDFFormat.NQUADS, null)
      stream.start()
      stream
    }(_.finish())
  }

  def determineOutGraph(modelIRI: String, config: Config): Option[String] =
    config.outputGraphName.map {
      name =>
        if (config.suffixGraph.bool) modelIRI + name
        else name
    }

  def findModelIRI(model: Model): Option[String] =
    model.listSubjectsWithProperty(RDF.`type`, OWL2.Ontology).asScala
      .find(_.isURIResource)
      .map(_.getURI)

  def loadFilterGraphQuery(path: String): RIO[Blocking, Query] =
    effectBlocking(QueryFactory.read(path))
      .filterOrFail(_.isAskType)(new Exception("Filter graph query must be ASK type"))

  def shouldUseGraph(query: Query, model: Model): Boolean =
    Using.resource(QueryExecutionFactory.create(query, model))(_.execAsk())

  final case class ModelFromPath(model: Model, path: String)

  sealed trait Result extends Product with Serializable {

    def path: String

  }

  final case class Inferences(path: String, graph: Option[Node], triples: Set[Triple]) extends Result

  final case class Inconsistent(path: String) extends Result

}
