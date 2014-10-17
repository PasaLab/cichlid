package nju.cichlid

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IntWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.storage.StorageLevel

object OWL{
  val parallism = 30
  var storagelevel = StorageLevel.MEMORY_ONLY
  val partitioner = new HashPartitioner(parallism)
  def addTransitive[A,B](s:Set[(A,B)]) = {
    s ++ (for ((x1,y1) <- s; (x2,y2) <- s if y1 == x2) yield (x1,y2))
  }
  def transitiveClosure[A,B](s:Set[(A,B)]):Set[(A,B)] = {
    val t = addTransitive(s)
    if(t.size == s.size) s else transitiveClosure(t)
  }
 def transitive(input:RDD[(Seq[Byte],Seq[Byte],Seq[Byte])]) = {
   var nextcount = 1L
   var p =input
   var q = p
   var l = q.map(t => ((t._2,t._3),t._1)).partitionBy(partitioner)
   do{
     val r = q.map(t => ((t._2,t._1),t._3)).partitionBy(partitioner) 
     val q1 = l.join(r).map(t => (t._2._1,t._1._1,t._2._2))
     q = q1.subtract(p,parallism).persist(storagelevel)
     nextcount= q.count
     if(nextcount!=0){
       l = q.map(t => ((t._2,t._3),t._1)).partitionBy(partitioner)
       val s = p.map(t => ((t._2,t._1),t._3)).partitionBy(partitioner)
       val p1=s.join(l).map(t => (t._2._2,t._1._1,t._2._1)).persist(storagelevel)
       p = p1.union(q).union(p)
     }
    }while(nextcount != 0)
    p
  }

  def sameAsTable(input:RDD[(Seq[Byte],Seq[Byte])]) = {
    //<groupid -personid>
    var owlsame = input
    var it = 0L
    var newit =owlsame.count
    do{
      // val owlsameright = owlsame.map(t => (t._2,t._1))
      val owlsameshuffle = owlsame.union(owlsame.map(t => (t._2,t._1)))
      val owltemp = owlsameshuffle.groupByKey(parallism).flatMap(
        t => {
          val min= t._2.min(
            new Ordering[Seq[Byte]] {
              def compare(a:Seq[Byte],b:Seq[Byte]) = 
            	a.hashCode compare b.hashCode
            }); 
          if(min.hashCode < t._1.hashCode){   
            for(arg <- t._2) yield (min,arg) 
          }else for(arg <- t._2) yield (t._1,arg)
        }).filter(t => !t._1.equals(t._2))
        owlsame = owltemp.persist(storagelevel)
        it = newit
        newit = owlsame.count()
    }while(it != newit)
    owlsame
  }
  def main(args: Array[String]): Unit = {
    if(args.length < 4 || args.length > 5){
       System.err.println("Usage:Reasoner<instance> <schema> <output> <StorageLevel> [<memoryFraction>]")
       System.exit(1);
    }
    val conf = new SparkConf().setAppName("Cichlid-OWL")
    val instanceFile = args(0)
    val schemaFile = args(1)
    val outputFile = args(2)
    storagelevel = args(3) match{
	  case "0" => StorageLevel.MEMORY_ONLY
	  case "1" => StorageLevel.MEMORY_AND_DISK
	  case "2" => StorageLevel.OFF_HEAP
	  case "3" => StorageLevel.DISK_ONLY
	  case "4" => StorageLevel.MEMORY_AND_DISK_SER
	  case "5" => StorageLevel.MEMORY_ONLY_SER
	  case "6" => StorageLevel.DISK_ONLY_2
	  case "7" => StorageLevel.MEMORY_AND_DISK_2
	  case "8" => StorageLevel.MEMORY_AND_DISK_SER_2
	  case "9" => StorageLevel.MEMORY_ONLY_2
	  case "10" => StorageLevel.MEMORY_ONLY_SER_2
	  case _ => StorageLevel.NONE
    }
    if(args.length == 5){
      conf.set("spark.storage.memoryFraction", args(4))
    }

    val sc=new SparkContext(conf)
    //read and resolve instance triples
    val text = sc.sequenceFile[BytesWritable,IntWritable](
      instanceFile,classOf[BytesWritable],classOf[IntWritable],parallism)
    var in = text
      .map(t => t._1.getBytes())
      .map(t=> (t.slice(0, 8).toSeq,t.slice(8, 16).toSeq,t.slice(16, 24).toSeq))
    //read and resolve ontology triples
    val schema = sc.sequenceFile[BytesWritable,IntWritable](
      schemaFile,classOf[BytesWritable],classOf[IntWritable])
    val ins= schema
      .map(t => t._1.getBytes())
      .map(t=> (t.slice(0, 8).toSeq,t.slice(8, 16).toSeq,t.slice(16, 24).toSeq))
    //RDFS reasoning: process ontology data
    val subprop = ins
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
    val subprops = transitiveClosure(subprop.toSet)//rule 5
    val subclasses = transitiveClosure(subclass.toSet)//rule 11
    //(t._1,S_RDFS_SUBPROPERTY_OF, t._3) => (t._1, t._3) => transitiveClosure => toMap
    val sp =sc.broadcast(subprops.toMap)
    //(t._1,S_RDFS_SUBCLASS_OF, t._3) => (t._1, t._3) => transitiveClosure => toMap
    val cl = sc.broadcast(subclasses.toMap)
    //(t._1, S_RDFS_DOMAIN, t._3) => (t._1, t._3) => toMap
    val dm = sc.broadcast(domain.toMap)
    //(t._1, S_RDFS_RANGE, t._3) => (t._1, t._3) => toMap
    val rg = sc.broadcast(range.toMap)
    val d = sp.value.map(t => (t._2,t._1))
    //OWL reasoning: process ontology data
    // fp = (t._1, S_RDF_TYPE, S_OWL_FUNCTIONAL_PROPERTY) => t._1
    val fp = sc.broadcast(
      ins.filter(t => t._3.equals(Rules.S_OWL_FUNCTIONAL_PROPERTY))
        .filter(t => t._2.equals(Rules.S_RDF_TYPE))
        .map(t => t._1)
        .collect
    )
    // ip = (t._1, S_RDF_TYPE, S_OWL_INVERSE_FUNCTIONAL_PROPERTY) => t._1
    val ip = sc.broadcast(
      ins.filter(t => t._3.equals(Rules.S_OWL_INVERSE_FUNCTIONAL_PROPERTY))
        .filter(t => t._2.equals(Rules.S_RDF_TYPE))
        .map(t => t._1)
        .collect
    )
    // tv = (t._1, t._2, S_OWL_TRANSITIVE_PROPERTY) => t._1
    val tv = sc.broadcast(
      ins.filter(t => t._3.equals(Rules.S_OWL_TRANSITIVE_PROPERTY))
        .map(t => t._1)
        .collect
    )
    // sm = (t._1, t._2, S_OWL_SYMMETRIC_PROPERTY) => t._1
    val sm = sc.broadcast(
      ins.filter(t => t._3.equals(Rules.S_OWL_SYMMETRIC_PROPERTY))
        .map(t => t._1)
        .collect
    )
    // hv = (t._1, S_OWL_HAS_VALUE, t._3) => Map(t._1 -> t._3) 
    val hv =sc.broadcast(
      ins.filter(t => t._2.equals(Rules.S_OWL_HAS_VALUE))
        .map(t => (t._1,t._3))
        .collect
        .toMap
    )
    // op = (t._1, S_OWL_ON_PROPERTY, t._3) => Map(t._1 -> t._3) 
    val op = sc.broadcast(
      ins.filter(t => t._2.equals(Rules.S_OWL_ON_PROPERTY))
        .map(t => (t._1,t._3))
        .collect
        .toMap
    )
    // sv = (t._1, S_OWL_SOME_VALUES_FROM, t._3) => Map(t._1 -> t._3)
    val sv =sc.broadcast(
      ins.filter(t => t._2.equals(Rules.S_OWL_SOME_VALUES_FROM))
        .map(t => (t._1,t._3))
        .collect
        .toMap
    )
    //av = (t._1, S_OWL_ALL_VALUES_FROM, t._3) => Map(t._3 -> t._1)
    val av = sc.broadcast(
      ins.filter(t => t._2.equals(Rules.S_OWL_ALL_VALUES_FROM))
        .map(t => (t._3,t._1))
        .collect
        .toMap
    )
    val iv1 = ins
      .filter(t => t._2.equals(Rules.S_OWL_INVERSE_OF))
      .map(t => (t._1,t._3))
    val iv2 = iv1.map(t => (t._2,t._1))
    val iv = sc.broadcast(
      iv1.union(iv2)
        .collect
        .toMap
    )//(t._1, S_OWL_INVERSE_OF, t._3) => (t._1, t._3), (t._3, t._1) => toMap
    //
    var tr = List[RDD[(Seq[Byte],Seq[Byte],Seq[Byte])]]()
    var tp = List[RDD[(Seq[Byte],Seq[Byte])]]()
    var sa = List[RDD[(Seq[Byte],Seq[Byte])]]()
    //split input instance data into three partitions
    var triples = in.filter(t => !(
      t._2.equals(Rules.S_OWL_SAME_AS) || t._2.equals(Rules.S_RDF_TYPE)
      ))
    var types = in
      .filter(t => (t._2.equals(Rules.S_RDF_TYPE)))
      .map(t => (t._1,t._3))
    var sames = in
      .filter(t => (t._2.equals(Rules.S_OWL_SAME_AS)))
      .map(t => (t._1,t._3))

    //loop reasoning process
    var flag = true
    while(flag){
      //process single instance rule triples
      //rule 7
      tp = Nil
      tr =Nil
      //RDFS Rule 7
      //s p o & p rdfs:subPropertyOf q => s q o
      val r7_t = triples
        .filter(t => sp.value.contains(t._2))
        .map(t => (t._1,sp.value(t._2),t._3))
        .persist(storagelevel)
      val r7_out = r7_t.union(triples)
      //RDFS rule 2 and 3
      //s p o & p rdfs:domain x => s rdf:type x
      //s p o & p rdfs:range  x => o rdf:type x
      val r2_out = r7_out
        .filter(t => dm.value.contains(t._2))
        .map(t => (t._1,dm.value(t._2)))
      val r3_out = r7_out
        .filter(t => rg.value.contains(t._2))
        .map(t => (t._3,rg.value(t._2)))
      val out_23 = r2_out.union(r3_out)
      tp = out_23::tp
      //s rdf:type x & x rdfs:subClassOf y => s rdf:type y
      val r9_out = out_23
        .union(types)
        .filter(t => cl.value.contains(t._2))
        .map(t => (t._1,cl.value(t._2)))
      tp = r9_out::tp
      //u p v & v owl:hasValue w & v owl:onProperty p => u rdf:type v
      val in14a = triples
        .filter(t => (
          op.value.contains(t._3) &&
          op.value(t._3).equals(t._2) &&
          hv.value.contains(t._3)
        ))
      val r14a = in14a.map(t => (t._1,t._3))
      tp = r14a::tp
      //u rdf:type v & v owl:hasValue w & v owl:onProperty p => u p v
      val in14b = types
        .filter(t => (
          hv.value.contains(t._2) &&
          op.value.contains(t._2)
        ))
      val r14b = in14b.map(t => (t._1,op.value(t._2),t._2))
      tr = r14b::tr
      //p owl:inverseOf q & v p w => w q v
      //p owl:inverseOf q & v q w => w p v
      val r8 = triples
        .filter(t => iv.value.contains(t._2))
        .map(t => (t._3,iv.value(t._2),t._1))
      tr = r8::tr
      //v p u & p rdf:type owl:SymmertricProperty => u p v
      var r3 = triples
        .filter(t => (sm.value.contains(t._2)))
        .map(t => (t._3,t._2,t._1))
      tr = r3::tr
      //deduplicate
      var newTriples = new UnionRDD(sc,tr)
        .distinct(parallism)
        .subtract(triples,parallism)
        .persist(storagelevel)
      var newTypes = new UnionRDD(sc,tp)
        .distinct(parallism)
        .subtract(types,parallism)
        .persist(storagelevel)
      var ctriples = newTriples.count
      var ctypes = newTypes.count
      //process multi-instance rules, sameAs rule and transitive rule
      if(ctypes!=0){
        types = types.union(newTypes)
        val op2 = op.value.map(t => (t._2,t._1))
        //v owl:onProperty p & u p x => <<v,x>,u>
        var in15 = triples
          .filter(t => op2.contains(t._2))
          .map(t => ((op2(t._2),t._3),t._1))
          .persist(storagelevel)
        //v owl:someValuesFrom w & x rdf:type w => <<v,x>,Nil>
        var t15 = types
          .filter(t => sv.value.contains(t._2))
          .map(t => ((sv.value(t._2),t._1),Nil))
          .persist(storagelevel)
        val r15 = in15.join(t15).map(t => (t._2._1,t._1._1))
        types = r15.union(types)
        //v owl:allValuesFrom u & v owl:onProperty p & w rdf:type v => <<w,p>,u>
        var in16 = types
          .filter(t => (av.value.contains(t._2) && op.value.contains(t._2)))
          .map(t => ((t._1,op.value(t._2)),av.value(t._2)))
          .persist(storagelevel)
        //v owl:allValuesFrom u & v owl:onProperty p & w p x => <<w,p>,x>
        var t16 = triples
          .filter(t => op2.contains(t._2) && av.value.contains(op2(t._2)))
          .map(t => ((t._1,t._2),t._3))
          .persist(storagelevel)
        val r16 = in16.join(t16).map(t => (t._2._2,t._2._1))//<x,u>
        types = r16.union(types)
     }
     //compute transitive rule
     if(ctriples!=0){
       triples = triples.union(newTriples)
       //rule 4--triple
       //p rdf:type owl:TransitiveProperty & u p w & w p v => u p v
       val r4_in = triples
         .filter(t => tv.value.contains(t._2))
         .persist(storagelevel)
       var r4 = transitive(r4_in)
       triples = r4.union(triples)                 
     }
     if(ctriples == 0 && ctypes==0){
       flag = false
     }      
   }//end of while(true)
  //process sameAs rule
  //horst_rule1  p rdf:type owl:FunctionalProperty & u p v & u p w => v owl:sameAs w
  val c1 = triples.filter(t => fp.value.contains(t._2))
  val in1 = c1.map(t => ((t._1,t._2),t._3))
  val r1 = in1.join(in1).map(t => (t._2._1,t._2._2)).filter(t => !t._1.equals(t._2))
  //Horst_2
  //p rdf:type owl:InverseFunctionalProperty & v p u & w p u => v owl:sameAs w
  val c2 = triples.filter(t => ip.value.contains(t._2))
  val in2 = c2.map(t => ((t._2,t._3),t._1))
  val r2 = in2.join(in2).map(t => (t._2._1,t._2._2)).filter(t => !t._1.equals(t._2))
  val in7 = new UnionRDD(sc,List(r1,r2,sames)).persist(storagelevel)
  val r7 = sameAsTable(in7).map(t => (t._2,t._1))
  val in11 = triples.map(t => (t._1,(t._2,t._3)))
  val rleft = in11.join(r7).map(t => (t._2._1._2,(t._2._2,t._2._1._1)))
  val r11 = rleft.join(r7).map(t => (t._2._1._1,t._2._1._2,t._2._2))
  val resultTypes = types.map(t => (t._1,Rules.S_RDF_TYPE,t._2))
  val resultSames = sames.map(t => (t._1,Rules.S_OWL_SAME_AS,t._2))
  val resultTriples = triples.union(r11)
  resultTypes.union(resultSames).union(resultTriples).saveAsTextFile(outputFile)
  }//end of main(args:String)
}
