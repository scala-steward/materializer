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
import zio.stream._

import java.io.{File, FileOutputStream, OutputStream}
import java.lang.System.currentTimeMillis
import scala.jdk.CollectionConverters._
import scala.util.Using

object Main extends ZCaseApp[Config] {

  private val ProvDerivedFrom = NodeFactory.createURI("http://www.w3.org/ns/prov#wasDerivedFrom")

  override def run(config: Config, arg: RemainingArgs): URIO[ZEnv, ExitCode] = {
    val managedWriter = Managed.fromAutoCloseable(ZIO.attempt(new FileOutputStream(new File(config.output))))
      .flatMap(createStreamRDF)
    val program = managedWriter.use { quadsWriter =>
      for {
        ontology <- loadOntology(config.ontologyFile)
        filterGraphQueryOpt <- ZIO.foreach(config.filterGraphQuery)(loadFilterGraphQuery)
        reasoner: Reasoner = ArachneReasoner(ontology, config.markDirectTypes.bool, config.outputIndirectTypes.bool)
        start <- ZIO.succeed(currentTimeMillis())
        inferencesStream = streamModels(config.input)
          .filter(model => filterGraphQueryOpt.exists(q => shouldUseGraph(q, model.model)))
          .take(100)
          .mapZIOParUnordered(config.parallelism) { model =>
            for {
              modelIRI <- ZIO.fromOption(findModelIRI(model.model))
                .orElseFail(new Exception(s"Dataset with no ontology IRI in file ${model.path}"))
              graphOpt = determineOutGraph(modelIRI, config).map(NodeFactory.createURI)
              provenanceOpt = graphOpt.map(g => Triple.create(g, ProvDerivedFrom, NodeFactory.createURI(modelIRI)))
              inferred = reasoner.materialize(model.model)
            } yield (graphOpt, inferred ++ provenanceOpt)
          }
        res <- inferencesStream.foreach { case (graphOpt, triples) =>
          writeModel(graphOpt, triples, quadsWriter)
        }
        _ <- ZIO.succeed(scribe.info("Finished foreach"))
        stop <- ZIO.succeed(currentTimeMillis())
        _ <- ZIO.succeed(scribe.info(s"Reasoning done in: ${(stop - start) / 1000.0}s"))
      } yield res
    }
    //program.catchAllCause(c => ZIO.succeed(scribe.error(c.prettyPrint))).exitCode
    program.tapError(e => ZIO.succeed(e.printStackTrace())).exitCode
  }

  def writeModel(graphOpt: Option[Node], triples: Set[Triple], quadsWriter: StreamRDF): Task[Unit] =
    ZIO.attemptBlocking {
      graphOpt match {
        case Some(graph) => triples.foreach(t => quadsWriter.quad(Quad.create(graph, t)))
        case None        => triples.foreach(quadsWriter.triple)
      }
    }

  def loadOntology(path: String): Task[OWLOntology] = for {
    manager <- ZIO.attempt(OWLManager.createOWLOntologyManager())
    ontology <- ZIO.attemptBlocking(manager.loadOntology(IRI.create(new File(path))))
    _ = scribe.info("Loaded ontology")
  } yield ontology

  def streamModels(path: String): Stream[Throwable, ModelFromPath] = {
    val fileOrDirectory = new File(path)
    val filesZ = for {
      isDirectory <- ZIO.attempt(fileOrDirectory.isDirectory)
      inputFiles <- if (isDirectory)
        ZIO.attempt(FileUtils.listFiles(fileOrDirectory, null, true).asScala.to(List))
      else ZIO.succeed(List(fileOrDirectory))
    } yield inputFiles
    ZStream.fromIterableZIO(filesZ).flatMapPar(16)(file => loadModelsFromFile(file.getPath))
    //ZStream.fromIterableZIO(filesZ).flatMap(file => loadModelsFromFile(file.getPath))
  }

  def loadModelsFromFile(filePath: String): Stream[Throwable, ModelFromPath] = {
    val models = ZIO.attemptBlocking(RDFDataMgr.loadDataset(filePath)).map { dataset =>
      val defaultModel = ModelFromPath(dataset.getDefaultModel, filePath)
      val namedModels = dataset.listNames().asScala.map(graphName => ModelFromPath(dataset.getNamedModel(graphName), filePath)).to(List)
      defaultModel :: namedModels
    }
    ZStream.fromIterableZIO(models)
  }

  def createStreamRDF(output: OutputStream): TaskManaged[StreamRDF] = {
    Managed.acquireReleaseAttemptWith {
      val stream = StreamRDFWriter.getWriterStream(output, RDFFormat.NQUADS, null)
      stream.start()
      stream
    } { stream =>
      stream.finish()
      scribe.info("Finish stream")
    }
  }

  def determineOutGraph(modelIRI: String, config: Config): Option[String] =
    config.outputGraphName.map { name =>
      if (config.suffixGraph.bool) modelIRI + name
      else name
    }

  def findModelIRI(model: Model): Option[String] =
    model.listSubjectsWithProperty(RDF.`type`, OWL2.Ontology).asScala
      .find(_.isURIResource)
      .map(_.getURI)

  def loadFilterGraphQuery(path: String): Task[Query] =
    ZIO.attemptBlocking(QueryFactory.read(path))
      .filterOrFail(_.isAskType)(new Exception("Filter graph query must be ASK type"))

  def shouldUseGraph(query: Query, model: Model): Boolean =
    Using.resource(QueryExecutionFactory.create(query, model))(_.execAsk())

  final case class ModelFromPath(model: Model, path: String)

}
