import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.util.{Failure, Success, Try}

object BigDataProject {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setAppName("Dataset Test - test2")
      /*need to be commented when run on Isabelle!*/
      conf.setMaster("local[2]")
      val sc = new SparkContext(conf)
      val ss = SparkSession.builder().config(conf).getOrCreate()
      import ss.implicits._
      //hide info
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      //spark.default.parallelism is the default number of partition set by spark which is by default 200.
      // and if you want to increase the number of partition than you can apply the property spark.sql.shuffle.partitions
      // to set number of partition in the spark configuration or while running spark SQL.
      // from https://stackoverflow.com/questions/45704156/what-is-the-difference-between-spark-sql-shuffle-partitions-and-spark-default-pa
      conf.set("spark.default.parallelism", "600")
      conf.set("spark.sql.shuffle.partitions", "300")


      // Structured Data, so dataframe is used here

      // for local testing: change to your own directory
      val DF = ss.read.format("com.databricks.spark.csv").option("sep", "\t").option("header", "true").load("/home/liangliang/IdeaProjects/BigData_1stWPO/src/main/scala/sample_us.tsv").persist();

      // for cluster testing
//      val DF = ss.read.format("com.databricks.spark.csv").option("sep", "\t").option("header", "true").load("hdfs://10.0.0.1:54310/data/amazon/")

      // Task 1. features selection
      // Baseline: Choosing 4 attributes that could be useful

      /*Turn review_body text to numerical length*/
      val subDF = DF.withColumn("review_body", length(col("review_body")))

      /*Turn vine and verified_purchase to binary number*/
      val subDF1 = subDF.withColumn("vine", when(col("vine").equalTo("Y"), 1).otherwise(0));
      val subDF2 = subDF1.withColumn("verified_purchase", when(col("verified_purchase").equalTo("Y"), 1).otherwise(0))

      /*Turn star_rating >=3 to 1 else to 0*/
      val subDF3 = subDF2.withColumn("star_rating", when(col("star_rating").leq(2), 0).otherwise(1))

      /*Turn Helperful/total > 0.5 to 1 otherwise 0*/
      val subDF4 = subDF3.withColumn("helpful_ratio", when(col("total_votes").equalTo(0), 0).otherwise(col("helpful_votes").divide(col("total_votes"))))
      val subDF5 = subDF4.withColumn("helpful_ratio", when(col("helpful_ratio").geq(0.5), 1).otherwise(0))

      /*review_body 1-100 becomes 1 101->200 becomes 2  larger than 200 becomes 3*/
      val subDF6 = subDF5.withColumn("review_body", when(col("review_body").leq(100), 0).when(col("review_body").geq(201), 2).otherwise(1))

      val subDF7 = subDF6.select( "helpful_ratio", "verified_purchase", "review_body", "vine", "star_rating")


      /*split data to test and training set*/
      val Array(train_df, test_df) = subDF7.randomSplit(Array(0.7, 0.3))
      val commonLabel = train_df.groupBy("star_rating").count().orderBy(desc("count")).first()(0).toString.toInt
      test_df.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      train_df.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      var ColumNameList = List("helpful_ratio", "review_body", "vine", "verified_purchase")


      //Task 2. Decision Tree Algorithm
      //  1. Entropy
      //  2. Informatio Gain
      //  3. Decision Tree Algorithm
      def log2(x: Double): Double = scala.math.log(x) / scala.math.log(2)

      val stringToDouble = udf((data: String) => {
          Try (data.toDouble) match {
              case Success(value) => value
              case Failure(exception) => Double.NaN
          }
      })

      /*Calculate Information Gain*/
      // Return the attrubute string with highest information gain
      def IG(data: DataFrame, target_id: String, Attributes: List[String]):String = {
          val Hs = H(data, target_id)

          //totalCount of this DataFrame
          val totalcount = data.count()
          var max = -1.0
          var maxString = ""

          for (a <- Attributes){
                // ratio of class in a certain attribute

                val attributeClassRatio = data.groupBy(col(a)).count()
                                              .withColumn("ratio", col("count").divide(totalcount))
                                              .withColumn("ratio", stringToDouble(col("ratio")).cast(DoubleType))
                                              .rdd.map(row => (row.getDouble(2), row(0))).collect().toList// use of collect could be justified, since after map count and ratio, the element should be no more than 5 in this case.

                var sum = 0.0

                // loop over all the categories
                for((pt,x2) <- attributeClassRatio) {
                    val cg = data.filter(col(a) === x2).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
                    val Ht = H(cg, target_id)
                    sum = sum + pt * Ht
                }

                var ig = Hs - sum
                if (ig > max) {
                    max = ig
                    maxString = a
                }
          }

          maxString
      }

      /*Tree Data Structure*/
      abstract class Tree

      case class LeafTree(Attribute_value: Any) extends Tree{
          var value: Any = Attribute_value
      }

      case class DecisionTree(Attribute: String) extends Tree{
          var subTree: List[TreeNode] = List()

          def addSubtree(Atrribute_value: Any, Tree: Tree): Unit ={
              subTree :+= TreeNode(Tree, Atrribute_value)
          }
      }

      case class TreeNode(Tree: Tree, Attribute_value: Any)

      def getAttributeValue(AttributeString: String): Int = {
          if (AttributeString.equals("helpful_ratio")){
              return 0
          }else if (AttributeString.equals("verified_purchase")){
              return 1
          }else if (AttributeString.equals("review_body")){
              return 2
          }else
              return 3
      }

      def predict(dataFrame: Row, Tree: Tree): Int ={
          var Tree1 = Tree
          while(Tree1.isInstanceOf[DecisionTree]){
              val Node:DecisionTree = Tree1.asInstanceOf[DecisionTree]
              val value = dataFrame.get(getAttributeValue(Node.Attribute))
              Tree1 = Node.subTree.filter(x => x.Attribute_value == value)(0).Tree
          }
          val y = Tree1.asInstanceOf[LeafTree].value.toString.toInt

          if(dataFrame.get(4).toString.toInt != y){
              return 0
          }
          return 1
      }

      def possibleValue(attribute: String): Array[Int] =  {
        if (attribute == "review_body"){
             Array(0,1,2)
        }else{
             Array(0,1)
        }
      }

      /*Calculate Entropy of dataset*/
      def H(data: DataFrame, columnName: String) = {
          var impurity = 0.0

          val totalcount = data.count()



          val data1 = data.groupBy(columnName)
              .count().withColumn("ratio", col("count")
              .divide(totalcount))
              .select(col("ratio"))
              .withColumn("ratio", stringToDouble(col("ratio")).cast(DoubleType))
              .rdd.map(row => row.getDouble(0)).collect().toList// use of collect could be justified, since after map count and ratio, the element should be no more than 5 in this case.

          for(x <- data1){
              impurity = impurity - (x * log2(x))
          }
          impurity
      }

      //mostCommonvalue test
      def mostCommonvalue(data: DataFrame, target: String): Any = {
          // mostCommonValue最后为空 需要修改如果只有一类直接返回
          var msv = 0
          if(data.take(1).isEmpty){
            msv = commonLabel
          }else {
            msv = data.groupBy(target).count().orderBy(desc("count")).first()(0).toString.toInt
          }
          msv
      }

      /*Decision Tree algorithm RECURSIVE*/
      //The set of examples that need to be classified is smaller than some threshold
      def ID3(data: DataFrame, target: String, attributes: List[String]):Tree = {
          if(data.count() < 2 || attributes.isEmpty || H(data, target) <= 0){
              return new LeafTree(mostCommonvalue(data, target))
          }else{
              // find the attribute that best classified the data
              val A = IG(data, target, attributes)

              val tree = new DecisionTree(A)
              //construct a new decision tree with root attribute A

              for(v <- possibleValue(A)){
                  val filtered = data.filter(col(A) === v).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
                  val subtree = ID3(filtered, target, attributes.filter(_ != A))

                  tree.addSubtree(v, subtree)
              }
              return tree
          }
      }


      // ID3 training part
      val Tree1 = ID3(train_df, "star_rating", ColumNameList)

      // Testing
      val prediction = test_df.map(row => (predict(row, Tree1))).reduce(_+_)
      println(prediction)
      val total = test_df.count()
      println(prediction.toFloat / total)

  }
}
