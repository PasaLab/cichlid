package nju.cichlid

import java.security.MessageDigest

object Rules {
    val hashLen = 8;
   def getHashValue(triple:String) = {
      val m = MessageDigest.getInstance("MD5")
      m.reset()
      //val b=triple.getBytes("UTF-8")
      m.update(triple.getBytes())
      val result = m.digest()
      result.slice(result.length-hashLen, result.length).toSeq
    }
  val S_OWL_CLASS = getHashValue("<http://www.w3.org/2002/07/owl#Class>")
  val S_OWL_FUNCTIONAL_PROPERTY = getHashValue("<http://www.w3.org/2002/07/owl#FunctionalProperty>")
  val S_OWL_INVERSE_FUNCTIONAL_PROPERTY = getHashValue("<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>")
  val S_OWL_SYMMETRIC_PROPERTY =getHashValue("<http://www.w3.org/2002/07/owl#SymmetricProperty>")
  val S_OWL_TRANSITIVE_PROPERTY = getHashValue("<http://www.w3.org/2002/07/owl#TransitiveProperty>")
  val S_OWL_SAME_AS = getHashValue("<http://www.w3.org/2002/07/owl#sameAs>")
  val S_OWL_INVERSE_OF = getHashValue("<http://www.w3.org/2002/07/owl#inverseOf>")
  val S_OWL_EQUIVALENT_CLASS =getHashValue("<http://www.w3.org/2002/07/owl#equivalentClass>")
  val S_OWL_EQUIVALENT_PROPERTY = getHashValue("<http://www.w3.org/2002/07/owl#equivalentProperty>")
  val S_OWL_HAS_VALUE = getHashValue("<http://www.w3.org/2002/07/owl#hasValue>")
  val S_OWL_ON_PROPERTY = getHashValue("<http://www.w3.org/2002/07/owl#onProperty>")
  val S_OWL_SOME_VALUES_FROM = getHashValue("<http://www.w3.org/2002/07/owl#someValuesFrom>")
  val S_OWL_ALL_VALUES_FROM = getHashValue("<http://www.w3.org/2002/07/owl#allValuesFrom>")
  val S_RDFS_PROPERTY = getHashValue("<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>")
  val S_RDFS_SUBCLASS_OF =getHashValue("<http://www.w3.org/2000/01/rdf-schema#subClassOf>")
  val S_RDFS_SUBPROPERTY_OF =getHashValue("<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>")
  val S_RDFS_DOMAIN = getHashValue("<http://www.w3.org/2000/01/rdf-schema#domain>")
  val S_RDFS_RANGE=getHashValue("<http://www.w3.org/2000/01/rdf-schema#range>")
  val S_RDF_TYPE=getHashValue("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")

}