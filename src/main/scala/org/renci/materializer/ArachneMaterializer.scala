package org.renci.materializer

import org.apache.jena.graph.{Node, NodeFactory, Node_URI, Triple}
import org.apache.jena.rdf.model.Model
import org.apache.jena.vocabulary.{OWL2, RDF, RDFS, XSD}
import org.geneontology.jena.OWLtoRules
import org.geneontology.rules.engine
import org.geneontology.rules.engine.{RuleEngine, URI}
import org.geneontology.rules.util.Bridge
import org.semanticweb.owlapi.model.OWLOntology
import org.semanticweb.owlapi.model.parameters.Imports

import scala.jdk.CollectionConverters.IteratorHasAsScala

class ArachneMaterializer(ontology: OWLOntology, markDirectTypes: Boolean, assertIndirectTypes: Boolean) extends Materializer {

  private val IndirectType = NodeFactory.createURI(OWLtoRules.IndirectType)
  private val ontRules = Bridge.rulesFromJena(OWLtoRules.translate(ontology, Imports.INCLUDED, true, true, false, true)).to(Set)
  private val indirectRules = if (markDirectTypes || !assertIndirectTypes)
    Bridge.rulesFromJena(OWLtoRules.indirectRules(ontology)).to(Set)
  else Set.empty
  private val arachne = new RuleEngine(ontRules ++ indirectRules, false)

  override def materialize(model: Model, allowInconsistent: Boolean): Option[Set[Triple]] = {
    val triples = model.listStatements().asScala.map(t => Bridge.tripleFromJena(t.asTriple())).to(Iterable)
    val wm = arachne.processTriples(triples)
    val inferred = wm.facts.to(Set) -- wm.asserted
    if (!allowInconsistent && isInconsistent(inferred)) None
    else {
      val jenaTriples = inferred.map(Bridge.jenaFromTriple)
      val finalTriples = if (markDirectTypes || !assertIndirectTypes) {
        val (indirectTypeTriples, otherTriples) = jenaTriples.partition(t => t.predicateMatches(IndirectType))
        val indirectRDFTypeTriples = indirectTypeTriples.map(t => Triple.create(t.getSubject, RDF.`type`.asNode(), t.getObject))
        val maybeWithoutIndirects = if (!assertIndirectTypes) {
          otherTriples -- indirectRDFTypeTriples
        } else otherTriples
        if (markDirectTypes) {
          val assertedTypeTriples = model.listStatements(null, RDF.`type`, null).asScala
            .map(_.asTriple()).filterNot(t => isBuiltIn(t.getObject)).toSet
          val assertedTypeTriplesThatAreDirect = assertedTypeTriples -- indirectRDFTypeTriples
          val allRemainingTypeTriples = maybeWithoutIndirects.filter(t => t.predicateMatches(RDF.`type`.asNode()))
          val directRDFTypeTriples = allRemainingTypeTriples -- indirectRDFTypeTriples
          val directTypeTriples = (directRDFTypeTriples ++ assertedTypeTriplesThatAreDirect).map(t => Triple.create(t.getSubject, DirectType, t.getObject))
          maybeWithoutIndirects ++ directTypeTriples
        } else maybeWithoutIndirects
      } else jenaTriples
      Some(finalTriples)
    }
  }

  private def isInconsistent(triples: Set[engine.Triple]): Boolean =
    triples.exists(t => t.o == URI(OWL2.Nothing.getURI) && t.p == URI(RDF.`type`.getURI))

  private def isBuiltIn(node: Node): Boolean = {
    node match {
      case n: Node_URI => n.getURI.startsWith(OWL2.NS) || n.getURI.startsWith(RDFS.getURI) || n.getURI.startsWith(RDF.getURI) || n.getURI.startsWith(XSD.getURI)
      case _           => false
    }
  }


}

object ArachneMaterializer {

  def apply(ontology: OWLOntology, markDirectTypes: Boolean, assertIndirectTypes: Boolean): ArachneMaterializer =
    new ArachneMaterializer(ontology, markDirectTypes, assertIndirectTypes)

}