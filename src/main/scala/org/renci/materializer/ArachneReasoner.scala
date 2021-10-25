package org.renci.materializer

import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.rdf.model.Model
import org.apache.jena.vocabulary.RDF
import org.eclipse.rdf4j.model.vocabulary.SESAME
import org.geneontology.jena.OWLtoRules
import org.geneontology.rules.engine.RuleEngine
import org.geneontology.rules.util.Bridge
import org.semanticweb.owlapi.model.OWLOntology
import org.semanticweb.owlapi.model.parameters.Imports

import scala.jdk.CollectionConverters.IteratorHasAsScala

//TODO inconsistent?
class ArachneReasoner(ontology: OWLOntology, markDirectTypes: Boolean, assertIndirectTypes: Boolean) extends Reasoner {

  private val DirectType = NodeFactory.createURI(SESAME.DIRECTTYPE.stringValue())
  private val IndirectType = NodeFactory.createURI(OWLtoRules.IndirectType)
  private val ontRules = Bridge.rulesFromJena(OWLtoRules.translate(ontology, Imports.INCLUDED, true, true, false, true)).to(Set)
  private val indirectRules = if (markDirectTypes || !assertIndirectTypes)
    Bridge.rulesFromJena(OWLtoRules.indirectRules(ontology)).to(Set)
  else Set.empty
  private val arachne = new RuleEngine(ontRules ++ indirectRules, false)

  def materialize(model: Model): Set[Triple] = {
    val triples = model.listStatements().asScala.map(t => Bridge.tripleFromJena(t.asTriple())).to(Iterable)
    val wm = arachne.processTriples(triples)
    val inferred = wm.facts.to(Set) -- wm.asserted
    val jenaTriples = inferred.map(Bridge.jenaFromTriple)
    if (markDirectTypes || !assertIndirectTypes) {
      val (indirectTypeTriples, otherTriples) = jenaTriples.partition(t => t.predicateMatches(IndirectType))
      val indirectRDFTypeTriples = indirectTypeTriples.map(t => Triple.create(t.getSubject, RDF.`type`.asNode(), t.getObject))
      val maybeWithoutIndirects = if (!assertIndirectTypes) {
        otherTriples -- indirectRDFTypeTriples
      } else otherTriples
      if (markDirectTypes) {
        val allRemainingTypeTriples = maybeWithoutIndirects.filter(t => t.predicateMatches(RDF.`type`.asNode()))
        val directRDFTypeTriples = allRemainingTypeTriples -- indirectRDFTypeTriples
        val directTypeTriples = directRDFTypeTriples.map(t => Triple.create(t.getSubject, DirectType, t.getObject))
        maybeWithoutIndirects ++ directTypeTriples
      } else maybeWithoutIndirects
    } else jenaTriples
  }

}

object ArachneReasoner {

  def apply(ontology: OWLOntology, markDirectTypes: Boolean, assertIndirectTypes: Boolean): ArachneReasoner =
    new ArachneReasoner(ontology, markDirectTypes, assertIndirectTypes)

}