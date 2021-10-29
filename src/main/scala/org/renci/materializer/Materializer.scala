package org.renci.materializer

import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.rdf.model.Model
import org.eclipse.rdf4j.model.vocabulary.SESAME

trait Materializer {

  val DirectType: Node = NodeFactory.createURI(SESAME.DIRECTTYPE.stringValue())

  def materialize(model: Model, allowInconsistent: Boolean): Option[Set[Triple]]

}
