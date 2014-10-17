package nju.cichlid

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IntWritable
import org.apache.spark.SparkConf
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.storage.StorageLevel

object RDFS {
  val parallism = 30
  val partitioner = new HashPartitioner(parallism)
  def addTransitive[A,B](s:Set[(A,B)]) = {
    s ++ (for ((x1,y1) <- s; (x2,y2) <- s if y1 == x2) yield (x1,y2))
  }
 //
  def transitiveClosure[A,B](s:Set[(A,B)]):Set[(A,B)] = {
    val t = addTransitive(s)
    if(t.size == s.size) s else transitiveClosure(t)
  }
  def main(args: Array[String]): Unit = {
    if(args.length < 3 || args.length > 4){
       System.err.println("Usage:Reasoner <instance> <schema> <output> [<memoryFraction>]")
       System.exit(1);
    }
    val conf = new SparkConf().setAppName("Cichlid-RDFS")
    val instanceFile = args(0)
    val schemaFile = args(1)
    val outputFile = args(2)
    if(args.length == 4){
      conf.set("spark.storage.memoryFraction", args(3))
    }
    val sc=new SparkContext(conf)
    val text = sc.sequenceFile[BytesWritable,IntWritable](
      instanceFile,classOf[BytesWritable],classOf[IntWritable],parallism
    )
    //parse triples
    val triples = text
      .map(t => t._1.getBytes())
      .map(t=> (t.slice(0, 8).toSeq,t.slice(8, 16).toSeq,t.slice(16, 24).toSeq))
    val stext =sc.sequenceFile[BytesWritable,IntWritable](
      schemaFile,classOf[BytesWritable],classOf[IntWritable]
    )
    val ins = stext
      .map(t => t._1.getBytes())
      .map(t=> (t.slice(0, 8).toSeq,t.slice(8, 16).toSeq,t.slice(16, 24).toSeq))
    //schema filter and load to memory
    val subprop =ins
      .filter(t => t._2.equals(Rules.S_RDFS_SUBPROPERTY_OF))
      .map(t => (t._1,t._3))
      .collect
    val subclass = ins
      .filter(t => t._2.equals(Rules.S_RDFS_SUBCLASS_OF))
      .map(t => (t._1,t._3))
      .collect
    val domain = ins
      .filter(t => t._2.equals(Rules.S_RDFS_DOMAIN))
      .map(t => (t._1,t._3))
      .collect
    val range = ins
      .filter(t => t._2.equals(Rules.S_RDFS_RANGE))
      .map(t => (t._1,t._3))
      .collect
    //p rdfs:subPropertyOf q &  q rdfs:subPropertyOf r => p rdfs:subPropertyOf r
    val subprops = transitiveClosure(subprop.toSet) 
    //x rdfs:subClassOf y & y rdfs:subClassOf z => x rdfs:subClassOf z
    val subclasses = transitiveClosure(subclass.toSet)
    //
    val sp =sc.broadcast(subprops.toMap)
    val cl = sc.broadcast(subclasses.toMap)
    val dm = sc.broadcast(domain.toMap)
    val rg = sc.broadcast(range.toMap)
    //rule 7: s p o & p rdfs:subPropertyOf q => s q o
    val r7_t = triples
      .filter(t => sp.value.contains(t._2))
      .map(t => (t._1,sp.value(t._2),t._3))
      .persist()
    val r7_out = r7_t.union(triples)
    //rule 2:p rdfs:domain x & s p o => s rdf:type x 
    val r2_out = r7_out
      .filter(t => dm.value.contains(t._2))
      .map(t => (t._1,dm.value(t._2)))
    //rule 3:p rdfs:range x & s p o => o rdf:type x
    val r3_out = r7_out
      .filter(t => rg.value.contains(t._2))
      .map(t => (t._3,rg.value(t._2)))
    //the result of rule 2 and rule 3
    val tp = triples
      .filter(t => t._2.equals(Rules.S_RDF_TYPE))
      .map(t => (t._1,t._3))
    val out_23 = r2_out.union(r3_out).union(tp)
    //rule 9:s rdf:type x & x rdfs:subClassOf y => s rdf:type y
    val r9_out = out_23
      .filter(t => cl.value.contains(t._2))
      .map(t => (t._1,cl.value(t._2)))
    val tpall = out_23
      .union(r9_out)
      .map(t =>(t._1,Rules.S_RDF_TYPE,t._2))
      .union(r7_t)
      .distinct(parallism)
    tpall.saveAsTextFile(outputFile)
  }
}