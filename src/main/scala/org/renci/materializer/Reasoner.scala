package org.renci.materializer

import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory, Statement}
import org.apache.jena.sparql.core.Quad

trait Reasoner {

  def materialize(model: Model): Set[Triple]

}
