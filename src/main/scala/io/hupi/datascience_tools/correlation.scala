// Fonction de conversion matrice to RDD
import org.apache.spark.rdd._

//mllib
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.Statistics

//spark context
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

//sparksql
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//general
import scala.collection.mutable.ListBuffer
import scala.math
import math._
import org.apache.commons.math3.distribution._

// For input Data Frame, make sure that all of column is String to apply these functions

package io.hupi.datascience_tools {

	object Correlation {
		// TEST PEARSON
		def pearson (coefficient : Double, numberElement : Long, alpha : Double) : String = {
		  //@param coefficient : correlation coefficient 
		  //@param numberElement : number of elements in sample
		  //@param alpha : error risk
		  
		  // Statistic of test follows a Student's t-distribution with degree of freedom of (numberElement-2)
		  val T = (coefficient * Math.pow(((numberElement-2)/(1-Math.pow(coefficient, 2))), 0.5)).abs
		  // Degree of Freedom
		  val degreeOfFreedom = numberElement - 2
		  // Critical Value of T 
		  val StudentLaw = new TDistribution(degreeOfFreedom)
		  val t = StudentLaw.inverseCumulativeProbability(1-(alpha/2))
		  // If T is larger than its critical value => the coefficient is significant
		  val result = if (T > t) "significant" else "not significant"
		  return result
		}

		// TEST FISHER
		def fisher (valueCompared : Double, coefficient : Double, numberElement : Long, alpha : Double) : String = {
		  //@param valueCompared : value to compare with the coefficient
		  //@param coefficient : correlation coefficient 
		  //@param numberElement : number of elements in sample
		  //@param alpha : error risk 
		  
		  // Transformation of Fisher
		  val z = 0.5 * Math.log((1.0 + coefficient)/(1.0 - coefficient))
		  // Intermediate calculations 
		  val z0 = 0.5 * Math.log((1 + valueCompared)/(1 - valueCompared))
		  // Statistic of test follows a standard normal distribution
		  val U = (z - z0) * Math.pow(numberElement - 3, 0.5)
		  //Critical Value of U
		  val NormalLaw = new NormalDistribution(0, 1)
		  val u = NormalLaw.inverseCumulativeProbability(1-(alpha))
		  // If U is larger than its critical value => the coefficient is significant
		  val res = if (U > u) "significant" else "not significant"
		  return res
		}

		// TEST OF SIGNIFICANCE
		def testSignificance (data: org.apache.spark.sql.DataFrame, coefficient : Double, alpha : Double, method : String) : String = method match {
		  //@param data : DataFrame that contains quantitative variables 
		  //@param coefficient : correlation coefficient 
		  //@param alpha : error risk
		  //@param method : "pearson" or "fisher"
		  
		  case "pearson" => pearson(coefficient, data.count(), alpha)
		  case "fisher" => fisher(0.0, coefficient, data.count(), alpha)
		  case _ => "Please retry only with method 'pearson' or 'fisher'" 
		}


		// Function of list of correlation coefficients
		def listCorrelation (data: org.apache.spark.sql.DataFrame, schemaData : Array[String], method : String) : List[(Double, (Int, Int))] = {
		  //@param data = DataFrame that contains quantitative variables 
		  //@param schemaData = Array[String] that contains quantitative variables names
		  //@param method = "pearson" / "spearman"  
		  
		  val numberVariables = schemaData.length 
		  val c = List.range(0, numberVariables)

		  val col1 = c flatMap { e => List.fill(numberVariables)(e) }
		  val col2 = List.fill(numberVariables)(c).flatten
		  val newData = col1.zip(col2)

		  val cor = new ListBuffer[Double]()

		  for (i <- 0 to (newData.length-1)) {
			val a = newData.map(l => l._1).toList(i) 
			val b = newData.map(l => l._2).toList(i) 
			val correlation = Statistics.corr(data.map(l => l(a).asInstanceOf[String].toDouble), data.map(l => l(b).asInstanceOf[String].toDouble), method)
			cor += correlation  
		  }

		  val table = newData.zip(cor).filter(l => l._2 < 0.9999).map(l => (l._2, l._1)).sortBy(_._1)
		  return table
		}

		// Function that returns the final result containing 3 columns (variables, correlation coefficient and result of its test of signicance) 
		def functionResult (variables : ListBuffer[String], coefficient : ListBuffer[Double], significance : ListBuffer[String]) : List[(String, Double, String)]= {
		  //@param variables = Variables  
		  //@param coefficient = Correlation coefficients
		  //@parma significance = Results of test of significance 
		  
		  val result = variables.zip(coefficient).zip(significance).toList.map(l => (l._1._1, l._1._2, l._2))
		  return result
		}




		def bestCorOf (data: org.apache.spark.sql.DataFrame, schemaData : Array[String], variable : String, method : String, 
					   test : String, alpha : Double, numberOfVariables : Int, direction : String, level : String) : List[(String, Double, String)] = {
		  //@param data = DataFrame that contains quantitative variables  
		  //@param schemaData = Array[String] that contains quantitative variables names
		  //@param variable = variable target
		  //@param method = "pearson" / "spearman" 
		  //@param test = "pearson" / "fisher"
		  //@param alpha = error risk
		  //@param numberOfVariables = number of variables
		  //@param direction = "negative" / "positive"
		  //@param level = "most" / "less"

		  val table = listCorrelation(data, schemaData, method)

		  val indexVariable = schemaData.indexOf(variable)
		  val choix = table.groupBy(_._2._1).getOrElse(indexVariable, null).map(l => (l._1, l._2._2))

		  val table_nega = choix.filter(l => l._1 < 0.0)
		  val table_posi = choix.filter(l => l._1 >= 0.0)

		  val newList = if (direction == "negative" && level == "most") 
						  table_nega 
						else if (direction == "positive" && level == "most") 
						  table_posi.reverse 
						else if (direction == "negative" && level == "less") 
						  table_nega.reverse 
						else 
						  table_posi

		  val finalList = if (numberOfVariables >= newList.length) newList else newList.take(numberOfVariables)
		  
		  val deg = finalList.map(l => l._1)
		  val v = finalList.map(l => l._2)
		  
		  val variables = new ListBuffer[String] ()
		  val degree = new ListBuffer[Double] ()
		  val significance = new ListBuffer[String] ()
		  
		  for (i <- 0 to (finalList.length - 1)) {
			val degre = deg(i)
			val variab = schemaData(v(i))
			val signif = testSignificance(data, degre, alpha, method) 
			variables += variab
			degree += degre
			significance += signif
		  }
		  return functionResult(variables, degree, significance)
		}




		def findBestCor (data: org.apache.spark.sql.DataFrame, schemaData : Array[String], method : String, test : String, alpha : Double, 
						 numberOfCouples : Int, direction : String, level : String) : List[(String, String, Double, String)] = {
		  //@param data = DataFrame that contains quantitative variables  
		  //@param schemaData = Array[String] that contains quantitative variables names
		  //@param method = "pearson" / "spearman" 
		  //@param test = "pearson" / "fisher"
		  //@param alpha = error risk
		  //@param numberOfVariables = number of variables
		  //@param direction = "negative" / "positive"
		  //@param level = "most" / "less"

		  val table = listCorrelation(data, schemaData, method)

		  val tablePropre = new ListBuffer[(Double, (Int, Int))]()

		  for (i <- 0 to (table.length-1) by 2) {
			tablePropre += table(i)
		  }

		  val table_nega = tablePropre.filter(l => l._1 < 0.0)
		  val table_posi = tablePropre.filter(l => l._1 >= 0.0)

		  val newList = if (direction == "negative" && level == "most") 
						  table_nega
						else if (direction == "positive" && level == "most")
						  table_posi.reverse 
						else if (direction == "negative" && level == "less")
						  table_nega.reverse 
						else
						  table_posi
		  val finalList = if (numberOfCouples >= newList.length) newList else newList.take(numberOfCouples)
		  
		  val valeur1 = finalList.map(l => l._1)
		  val var11 = finalList.map(l => l._2._1)
		  val var21 = finalList.map(l => l._2._2)

		  val variable1 = new ListBuffer[String] ()
		  val variable2 = new ListBuffer[String] ()
		  val degree = new ListBuffer[Double] ()
		  val significance = new ListBuffer[String] ()
		  
		  for (i <- 0 to (finalList.length - 1)) {
			val v = valeur1(i)
			val var1 = schemaData(var11(i))
			val var2 = schemaData(var21(i))
			val signif = testSignificance(data, v, alpha, test) 
			variable1 += var1
			variable2 += var2
			degree += v
			significance += signif
		  } 
		  val result = variable1.zip(variable2).zip(degree).zip(significance)
								.toList.map(l => (l._1._1._1, l._1._1._2, l._1._2, l._2))
		  return result
		}



		// Fonction de conversion matrice to RDD[Vector]
		def matrixToRddVector(m: Matrix, sc: SparkContext): RDD[Vector] = {
			val columns = m.toArray.grouped(m.numRows)
			val rows = columns.toSeq.transpose 
			val vectors = rows.map(row => new DenseVector(row.toArray))
			sc.parallelize(vectors)
		}

		// Fonction qui renvoie les coordonnées des individus dans le sous-espace de dimension réduite 
		def PCA_individual (data : org.apache.spark.rdd.RDD[List[Double]], sc: SparkContext) : org.apache.spark.rdd.RDD[Array[Double]] = { 
		  //@param input : RDD[Array[String]]
		  
		  val data_transposed = data.collect().toList.transpose
		  val nb_line = data.count().toInt
		  val nb_col = data.first().length 

		  val t = data_transposed.flatten.toArray

		  val d = Matrices.dense(nb_line, nb_col, t)

		  val rows = matrixToRddVector(d, sc)
		  val mat = new RowMatrix(rows)
			
		  // On diminue la dimension de 11 à 2
		  // Ici, les lignes correspondent à des observations et les colonnes des variables.
		  // Chaque colonne correspond à une composante principale
		  // les colonnes sont ordonnéés selon l'odre décroissant (de gauche à droite)
		  // On peut dire que ce sont les 2 componentes qui a l'inertie les plus importantes.
		  // Matrice de 2 composantes principales
		  val pc = mat.computePrincipalComponents(2)
		  
		  // Projeter des lignes dans l'espace linéaire créée par 2 dimensions principales
		  // Les axes déterminés par les Cp sont appelés "axes principaux"
		  // Les formes linéaires associées par les composantes principales sont appelées "facteurs principaux"
		  // les coordonnées des observations dans le sous-espace de dimension réduite (ici 2)
		  val projected = mat.multiply(pc)

		  // Normalement, on prend 2 composantes principales pour traces le graphique des individus statistiques
		  // qui permet de préciser la signification des axes et des facteurs.
		  
		  val res = projected.rows.map(l => l.toArray)
		  
		  return res
		}





		def corQualiQuanti (data: DataFrame, schemaData : Array[String], numberOfVariables : Int, level : String, method : String, 
							alpha : Double) : List[(String, Double, String)] = {
		  //@param data = DataFrame that contains qualitative variable (!!FIRST COLUMN!!) and quantitative variables 
		  //@param schemaData = Array[String] that contains variables names
		  //@param numberOfVariables = number of variables
		  //@param level = "most" / "less"
		  //@paral method = test of significance : "pearson" / "fisher"
		  //@param alpha = error risk
		  
		  val liste = new ListBuffer[Double]()
		  val nbQuantitatifs = schemaData.length
		  
		  for (varQuantitatif <- 1 to (nbQuantitatifs-1)) {
			val input = data.map(l => l(varQuantitatif).asInstanceOf[String].toDouble) 
			
			// Computation of total variation
			val list = new ListBuffer[Double]()
			val average = input.mean
			val varTotale = input.map(l => Math.pow(l - average, 2)).reduce(_+_)
			
			// Computation of inter-group's variation
			val list1 = new ListBuffer[Double]()
			val categories = data.map(l => l(0).asInstanceOf[String]).distinct().collect().sorted
			for (category <- categories) {
			  val group = data.map(l => (l(0).asInstanceOf[String], l(varQuantitatif)))
							  .filter(l => l._1.contains(category))
							  .map(l => l._2.asInstanceOf[String].toDouble) 

			  val v = group.count()*Math.pow(group.mean - average, 2)
			  list1 += v
			}
			val varInter = list1.reduceLeft(_ + _)
			
			// Relation coefficient
			val relationCoefficient = varInter/varTotale
			// This number is between 0 and 1. If it's close to 0 => two variables are independant
			
			liste += relationCoefficient
		  }
		  val withIndex = liste.zipWithIndex.sortBy(_._1) 

		  val liste1 = if (level == "most") 
						 withIndex.reverse 
					   else 
						 withIndex
		 
		  val nb1 = if (numberOfVariables < liste1.length) 
					  numberOfVariables 
					else 
					  liste1.length
		  
		  val variables = new ListBuffer[String] ()
		  val relation = new ListBuffer[Double] ()
		  val significance = new ListBuffer[String] ()
		  
		  val v = liste1.map(l => l._2)
		  val rel = liste1.map(l => l._1)
		  for (i <- 0 to (nb1 - 1)) {
			val r = rel(i)
			val variab = schemaData(v(i))
			val signif = testSignificance(data, r, alpha, method) 
			variables += variab
			relation += r
			significance += signif
		  }
		  return functionResult(variables, relation, significance)
		}


		def tableCoefShapiro(customer : String, sc: SparkContext) = {
		 sc.textFile(s"hdfs://$customer.node1.pro.hupi.loc:8020/user/$customer/stat/table/tableCoefShapiro.txt").map(l => l.split(",")).collect()
		}

		def tableStatShapiro(customer : String, sc: SparkContext) = {
		  sc.textFile(s"hdfs://$customer.node1.pro.hupi.loc:8020/user/$customer/stat/table/tableStatShapiro.txt").map(l => l.split(",")).collect()
		}

		def shapiro (data : org.apache.spark.rdd.RDD[(String, Double)], alpha : Double, customer : String, sc: SparkContext) : Int = {
		  //@param data = RDD[(String, Double)] sample to test normality   
		  //@param alpha = error risk
		  //@param customer = "hupi" to load statistic tables 
		  
		  val mean = data.map(l => l._2).mean
		  // Calculer la valeur S
		  val a = data.map(l => Math.pow(l._2 - mean, 2))
		  val S = a.reduce(_ + _)
		  val n = data.count().toInt

		  // Compute differences (max-min,..)  for shapiro's test
		  // if n is odd, we don't take the median
		  val nbCouples = if(data.count() % 2 == 0) n/2 else (n-1)/2
		  val k1 = data.map(l =>l._2).takeOrdered(nbCouples)
		  val k2 = data.map(l =>l._2).takeOrdered(nbCouples)(Ordering[Double].reverse)
		  val listDifference = k2.zip(k1).map(l => l._1 - l._2)    

		  // Load table of coefficients 
		  val tableCoef = tableCoefShapiro(customer, sc)
		  val listCoef = tableCoef.map(l => l(n-2)).map(_.toDouble).filter(l => l != 0.0)
		  val r = listDifference.zip(listCoef).map(l => l._1 * l._2).reduceLeft(_+_)

		  // Computation of statistic of test
		  val W = Math.pow(r, 2) / S

		  // Load statistic table for Shapiro's test
		  val tableShapiro = tableStatShapiro(customer, sc)
					  
		  // Critical Value for W
		  val res = if (alpha == 0.05){
			val w = tableShapiro.map(l => l(0).toDouble).toList(n-5) // because the table starts with column n = 5
			// if W > w => the sample follows a normal distribution
			if (W > w) 1 else 0
		  } else if (alpha == 0.01){
			val w = tableShapiro.map(l => l(1).toDouble).toList(n-5)
			// if W > w => the sample follows a normal distribution
			if (W > w) 1 else 0
		  } else {
			99999 // because the statistic table contains just 2 columns of alpha (0.05 and 0.01)
		  }
		  return res
		}

		def agostino (data : org.apache.spark.rdd.RDD[(String, Double)], alpha : Double) : Int = {
		  //@param data = RDD[(String, Double)] sample to test normality   
		  //@param alpha = error risk
		  
		  val n = data.count()
		  val mean = data.map(l => l._2).mean
		  // Transformation of asymmetry coefficient
		  val liste = data.map(l => (Math.pow(l._2 - mean, 2), Math.pow(l._2 - mean, 3)))
		  val g = liste.map(l => l._2).reduce(_+_)
		  val h = liste.map(l => l._1).reduce(_+_)
		  val g1 = (g/n) / Math.pow(h/n, 1.5)
		  val A = g1 * Math.sqrt(((n+1) * (n+3)) / (6 * (n-2)))
		  val B = (3 * (27*n-70+Math.pow(n, 2)) * (n+1) * (n+3)) / ((n-2) * (n+5) * (n+7) * (n+9))
		  val C = Math.sqrt(2 * (B - 1)) - 1
		  val E = 1 / Math.sqrt(Math.log(Math.sqrt(C)))
		  val F = A / Math.sqrt(2/(C-1))
		  val z1 = E * Math.log(F + Math.sqrt(Math.pow(F, 2) + 1))

		  // Transformation of kurtosis value
		  val stdev = data.map(l => l._2).stdev
		  val h1 = data.map(l => Math.pow((l._2 - mean) / stdev, 4)).reduce(_ + _)
		  val g2 = ((n * (n+1)) / ((n-1) * (n-2) * (n-3))) * h1 - ((3* Math.pow(n-1, 2)) / ((n-2) * (n-3)))
		  val G = (24 * n * (n-2) * (n-3)) / (Math.pow(n+1, 2) * (n+3) * (n+5))
		  val H = ((n-2) * (n-3) * g2) / ((n+1) * (n-1) * Math.sqrt(G))
		  val J = ((6 * (Math.pow(n, 2) - 5*n + 2)) / ((n.toDouble+7) * (n.toDouble+9))) * Math.pow((6 * (n.toDouble+3) * (n.toDouble+5)) / (n.toDouble * (n.toDouble-2) * (n.toDouble-3)), 0.5)
		  val K = 6 + (8/J) * ((2/J) + Math.pow(1 + (4 / Math.pow(J, 2)), 0.5))
		  val L = (1 - (2/K)) / (1 + H * Math.sqrt(2 / (K-4)))
		  val z2 = ((1 - (2 / (9*K))) - Math.pow(L, 1/3)) / Math.sqrt(2/(9*K))

		  // z1 and z2 follows a standard normal distribution
		  // Statistic of test will follow Chi-Squared distribution with degree of freedom equal to 2
		  val statTest = z1 + z2

		  // Critical Value 
		  val ChiSquaredLaw = new ChiSquaredDistribution(2)
		  val valueCritical = ChiSquaredLaw.inverseCumulativeProbability(1-(alpha))

		  // If statTest < valueCritical => the sample follows a normal distribution
		  val res = if (statTest < valueCritical) 1 else 0
		  return res
		}

		def testNormality (data : org.apache.spark.rdd.RDD[(String, Double)], test : String, alpha : Double, customer : String, sc: SparkContext) : Int = {
		  //@param data = RDD[(String, Double)] sample to test normality 
		  //@param test : "shapiro" ou "agostino"
		  //@param alpha = error risk
		  //@param customer = "hupi" to load statistic tables 
		   
		  val n = data.count() 
		  val res = if (n >= 30) 
					  1
					else if (test == "shapiro") { 
					  shapiro(data, alpha, customer, sc)
					} else if (test == "agostino") {
					  agostino(data, alpha)
					} else {
					  99999 // error when we use other tests 
					}  
		   return res
		}

		def testHomoscedasticity (data : org.apache.spark.rdd.RDD[(String, Double)], testnorm : String, alpha : Double, customer : String, sc : SparkContext) : Int = {
		  //@param data : RDD[(String, Double)] sample to test sample's homoscedasticity 
		  //@param testnorm : "shapiro" ou "agostino"
		  //@param alpha = error risk
		  //@param customer = "hupi" to load statistic tables 
		 
		  // if sample follows a normal distribution, we use Bartlett's test, if not we use Levene's test
		  val norm = testNormality(data, testnorm, alpha, customer, sc)
		  val numberLine = data.count()
		  val nameCol = data.map(l => l._1).distinct().collect()
		  val numberCol = nameCol.length
		  
		  if (norm == 1) {
			//Computation of statistic of test
			val g = new ListBuffer[Double]()
			val l = new ListBuffer[Double]()
			val m = new ListBuffer[Double]()

			for (i <- 0 to (numberCol-1)) {
			  val res = data.filter(l => l._1.contains(nameCol(i))).map(l => l._2)
			  val r = res.count()
			  val variance = res.variance
			  val ele = (r - 1) * variance
			  val ele1 = (r - 1) * Math.log(variance)
			  val ele2 = (1/(r-1)) - (1/(numberLine-numberCol))
			  g += ele
			  l += ele1
			  m += ele2
			}

			val s = g.reduceLeft(_ + _) / (numberLine-numberCol)
			val T1 = (numberLine - numberCol) * Math.log(s) - l.reduceLeft(_ + _)
			val T2 = 1 + (1/(3 * (numberCol-1))) * m.reduceLeft(_ + _) 
			val T = T1 / T2

			// In null hypothesis, T follows Chi-Squared's distribution with degree of freedom = numberCol - 1
			// Critical value of T
			val chiSquaredLaw = new ChiSquaredDistribution(numberCol-1)
			val valueCritical = chiSquaredLaw.inverseCumulativeProbability(1-(alpha))

			// If T >= valueCritical => there is at least 2 differents variances 
			val res = if (T < valueCritical) 1 else 0  
			return res   
			
		  } else {
			
			// Transformation of variables 
			val absDifference = new ListBuffer[Array[Double]]()
			for (i <- 0 to (numberCol-1)) {
			  val res = data.filter(l => l._1.contains(nameCol(i))).map(l => (l._2))
			  val m = res.mean
			  val z = res.map(l => (l - m).abs).collect()
			  absDifference += z
			}
			val meanDifference = absDifference.flatten.reduceLeft(_ + _) / absDifference.length
			
			// Statistic of test 
			val list1 = new ListBuffer[Double]()
			val list2 = new ListBuffer[Array[Double]]()
			for (i <- 0 to (numberCol-1)) {
			  val res = data.filter(l => l._1.contains(nameCol(i))).map(l => (l._1, l._2))
			  val r1 = res.map(l => l._2).mean
			  val r2 = res.count() * Math.pow(r1 - meanDifference, 2)
			  val r3 = absDifference.flatten.toArray.map(l => Math.pow(l - r1, 2))
			  list1 += r2
			  list2 += r3
			}
			  
			val W1 = (numberLine - numberCol) * list1.reduceLeft(_ + _)
			val W2 = (numberCol - 1) * list2.flatten.reduceLeft(_ + _)
			val W = W1 + W2

			// Critical value of W
			val fisherLaw = new FDistribution(numberCol-1, numberLine-numberCol)
			val valueCritical = fisherLaw.inverseCumulativeProbability(1-(alpha))

			val res = if (W < valueCritical) 1 else 0
			return res
		  }
		}


		def ANOVA (data : org.apache.spark.rdd.RDD[(String, Double)], alpha : Double, testNorm : String, client : String, sc: SparkContext) : Unit = {
		  //@param data : RDD[(String, Double)] sample to test correlation of 2 variables
		  //@param testnorm : "shapiro" ou "agostino"
		  //@param alpha = error risk
		  //@param customer = "hupi" to load statistic tables 
		  
		  val norm = testNormality(data, testNorm, alpha, client, sc) 
		  val homos = testHomoscedasticity(data, testNorm, alpha, client, sc)
		  
		  if (norm == 1 && homos == 1) {
			val numberLine = data.count()
			val nameCol = data.map(l => l._1).distinct().collect()
			val numberCol = nameCol.length
			val means = new ListBuffer[Double]()
			val stdev = new ListBuffer[Double]()
			
			// Computation of mean and standard deviation of sample
			for (i <- 0 to (numberCol-1)) {
			  val r = data.filter(l => l._1.contains(nameCol(i))).map(l => l._2)
			  val r1 = r.mean
			  val r2 = r.stdev
			  means += r1
			  stdev += r2
			}   

			// Computation of global mean
			val meanGlobal = means.reduceLeft(_ + _) / numberCol

			// Total variation = variation of factor + residual variation
			
			// -- Total variation = dispersion of elements around the global mean
			// -- Variation of factor = dispersion of categories mean around the global mean
			// -- Residual variation = dispersion of elements of each category around its mean (categories mean)

			// -- Variance totale = dispersion des données autour de la moyenne générale
			// -- Variance due au facteur = dispersion des moyennes autour de la moyenne générale
			// -- Variance résiduelle = dispersion des données à l'intérieur de chaque échantillon autour de sa moyenne.

			// Computation of variance of means 
			val v = new ListBuffer[Double]()
			for (i <- 0 to (numberCol-1)) {
			  val r = Math.pow(means(i) - meanGlobal, 2)
			  v += r
			}
			val varMeans = v.reduceLeft(_ + _) / numberCol

			// Computation of mean of variances
			val meanVar = stdev.reduceLeft(_ + _) / numberCol

			// Computation of global standard deviation
			val stdevGlobal = varMeans + meanVar

			// COmputation of total variance
			val varTotal = data.map(l => Math.pow(l._2 - meanGlobal, 2)).reduce(_+_)

			// Computation of residual variance
			val a1 = new ListBuffer[Array[Double]]()
			for (i <- 0 to (numberCol-1)) {
			  val a = data.filter(l => l._1.contains(nameCol(i))).map(l => Math.pow(l._2 - means(i), 2)).collect()
			  a1 += a
			}

			val varResidual = a1.flatten.reduceLeft(_ + _)

			// Computation of factor's variance
			val a2 = new ListBuffer[Double]()
			for (i <- 0 to (numberCol - 1)) {
			  val res = Math.pow(means(i)- meanGlobal, 2)
			  a2 += res
			}
			val varFactoriel = numberLine * (a2.reduceLeft(_ + _))

			// Test ANOVA
			// To compare 2 elements, we need to find the relation of mean squared (Factorial & Residual)
			val meanSquaredFactorial = varFactoriel / (numberCol-1)
			val meanSquaredResidual = varResidual / (numberLine-1)

			// Statistic of test (follows Fisher's distribution (K-1, N-K) in null hypothesis of Fisher's test)
			val F = meanSquaredFactorial / meanSquaredResidual

			// Critical Value of F
			val FisherDist = new FDistribution(numberCol-1, numberLine-numberCol)
			val f = FisherDist.inverseCumulativeProbability(1-(alpha))

			// if F > f => there is differents means in sample or 2 variables are independants
			if (F > f) {
			  println(s"Selon ANOVA, il y a une différence significative de la moyenne des modalités de la variable qualitative (indépendance de 2 variables) avec risque d'erreur de $alpha %")
			} else {
			  println(s"Selon ANOVA, il n'y a pas une différence significative de la moyenne de variable quantitative des modalités de la variable qualitative (deux variables sont liées) avec risque d'erreur de $alpha %")
			}   
		  } else {
			println ("On ne peut pas faire ANOVA avec cette échantillon car elle ne remplit pas les conditions demandées (normalité, homoscédascité)")
		  }
		}


		def corQualiQuali (data : org.apache.spark.rdd.RDD[(String, String)], alpha : Double) : Double = {
		  //@param data = RDD[(String, String)] that contains qualitatives variables
		  //@alpha : error risk
		  
		  val nameCategory1 = data.map(l => l._1).distinct().collect().sorted
		  val nameCategory2 = data.map(l => l._2).distinct().collect().sorted
		  val numberCategory1 = data.map(l => l._1).distinct().count().toInt
		  val numberCategory2 = data.map(l => l._2).distinct().count().toInt

		  // Computation of observed number 
		  val col1 = List.fill(numberCategory2)(nameCategory1).flatten
		  val col2 = nameCategory2 flatMap { e => List.fill(numberCategory1)(e)}
		  val newData = col1.zip(col2)
		  val category1 = newData.map(l => l._1)
		  val category2 = newData.map(l => l._2)

		  val numberObserved = new ListBuffer[Double]()

		  for (i <- 0 to (newData.length-1)) {
			val eff  = data.filter(l => l._1.contains(category1(i))).filter(l => (l._2.contains(category2(i)))).count().toDouble
			numberObserved += eff
		  }

		  // Computation of theoric number
		  val total1 = new ListBuffer[Double]()
		  val total2 = new ListBuffer[Double]()

		  for (i <- 0 to (numberCategory1-1)) {
			val valeur = data.filter(l => l._1.contains(nameCategory1(i))).count().toDouble
			total1 += valeur
		  }
		  for (i <- 0 to (numberCategory2-1)) {
			val valeur = data.filter(l => l._2.contains(nameCategory2(i))).count().toDouble
			total2 += valeur
		  }

		  val column1 = List.fill(numberCategory2)(total1).flatten
		  val column2 = total2 flatMap { e => List.fill(numberCategory1)(e)}
		  val newData1 = column1.zip(column2)
		  
		  val numberTheoric = newData1.map(l => (l._1 * l._2) / data.count())
		  
		  // Statistic of test
		  val Q = numberTheoric.zip(numberObserved).map(l => (Math.pow(l._2 - l._1, 2)/l._1)) . reduceLeft(_+_)

		  // Degree of Freedom
		  val degreeOfFreedom = (numberCategory1 - 1) * (numberCategory2 - 1)

		  // Intensity of relation : measured by Tschuprow's coefficient (this relation is between 0 and 1)
		  val T = Math.sqrt(Q / (data.count() * Math.sqrt(degreeOfFreedom)))  

		  // Critical value of Q
		  val ChiSquaredLaw = new ChiSquaredDistribution(degreeOfFreedom)
		  val valueCritical = ChiSquaredLaw.inverseCumulativeProbability(1-(alpha))
		  
		  // if Q > res, 2 variables are independent with error risk = alpha %
		  val result = if (Q > valueCritical) T else 0.0
		  return result
		}


		def corQualiOf (data : org.apache.spark.rdd.RDD[Array[String]], variables : Array[String], alpha : Double, 
						variable : String) : List[(String, Double)] = {
		  
		  //@param data = RDD[Array[String]] that contains qualitatives variables
		  //@param variables = list of variables
		  //@param alpha = error risk
		  //@param variable = variable target
		  
		  val numberVariables = variables.length
		  val indexVariables = List.range(0, numberVariables)
		  val col1 = indexVariables flatMap { e => List.fill(numberVariables)(e) }
		  val col2 = List.fill(numberVariables)(indexVariables).flatten
		  val newData = col1.zip(col2).filter(l => l._1 != l._2)

		  val relation = new ListBuffer[Double]()
		  for (i <- 0 to (newData.length-1)) {
			val e1 = newData.map(l => l._1).toList(i)
			val e2 = newData.map(l => l._2).toList(i)
			val d = data.map(l => (l(e1), l(e2)))
			val res = corQualiQuali(d, alpha)
			relation += res
		  }

		  val d = variables.indexOf(variable)
		  val listPropre = relation.zip(newData).sortBy(_._1)
									.map(l => (l._2._1, (l._2._2, l._1)))
									.groupBy(_._1)
									.getOrElse(d, null)
									.map(l => l._2)

		  val variab = listPropre.map(l => l._1)
		  val rel = listPropre.map(l => l._2)
		  
		  val listVariables = new ListBuffer[String] ()
		  val listRelation = new ListBuffer[Double] ()

		  for (i <- 0 to (listPropre.length - 1)) {
			val v1 = variables(variab(i))
			val deg = rel(i)
			listVariables += v1
			listRelation += deg
		  }
		  val result = listVariables.zip(listRelation).toList
		  return result
		}

		// Fonction qui renvoie les coordonnées des variables dans le nouvel sous-espace avec la dimension réduite de 2
		def FCA (data : org.apache.spark.rdd.RDD[(String, String)], profil : String, sc: SparkContext) : Array[Array[Double]] = { 
		  //@param data : org.apache.spark.rdd.RDD[(String, String)]
		  //@param profil : String = "colonne" / "ligne"

		  // Construction de la table de contingence
		  val nameCategory1 = data.map(l => l._1).distinct().collect().sorted
		  val nameCategory2 = data.map(l => l._2).distinct().collect().sorted
		  val numberCategory1 = data.map(l => l._1).distinct().count().toInt
		  val numberCategory2 = data.map(l => l._2).distinct().count().toInt

		  // Calcul des effectifs observés
		  val col1 = List.fill(numberCategory2)(nameCategory1).flatten
		  val col2 = nameCategory2 flatMap { e => List.fill(numberCategory1)(e)}
		  val newData = col1.zip(col2)
		  val category1 = newData.map(l => l._1)
		  val category2 = newData.map(l => l._2)

		  val numberObserved = new ListBuffer[Double]()

		  for (i <- 0 to (newData.length-1)) {
			val eff  = data.filter(l => l._1.contains(category1(i))).filter(l => (l._2.contains(category2(i)))).count().toDouble
			numberObserved += eff
		  }

		  val t1 = numberObserved.toArray  
		  val matrixContin = Matrices.dense(numberCategory1, numberCategory2, t1)
		  val rows1 = matrixToRddVector(matrixContin, sc)
		  // mat1 = table de contingence
		  val mat1 = new RowMatrix(rows1)

		  // Calculer la matrice des profils-lignes
		  val p1 = new ListBuffer[Array[Double]]()

		  for (i <- 0 to (numberCategory1-1)) {
			val m = mat1.rows.map(l => l.toArray).collect()(i)
			val r = m.map(l => l/m.reduceLeft(_ + _))
			p1 += r
		  }

		  val pLine = p1.flatten.toList

		  val pLine1 = pLine.toArray
		  val matrix1 = Matrices.dense(numberCategory2, numberCategory1, pLine1)
		  val matrixTransposed = matrix1.transpose
		  val rows2 = matrixToRddVector(matrixTransposed, sc)
		  // X = matrice des profils-lignes
		  val matrixLine = new RowMatrix(rows2)

		  // Calculer la matrice des profils-colonnes
		  val matrixContinTransposed = matrixContin.transpose
		  val rows11 = matrixToRddVector(matrixContinTransposed, sc)
		  val mat11 = new RowMatrix(rows11)

		  val p2 = new ListBuffer[Array[Double]]()
		  val prof_colonne = new ListBuffer[Double]()

		  for (i <- 0 to (numberCategory2-1)) {
			val m = mat11.rows.map(l => l.toArray).collect()(i)
			val r = m.map(l => l/m.reduceLeft(_ + _))
			p2 += r
		  }

		  val pCol = p2.flatten.toList

		  val pColumn = pCol.toArray
		  val matrix2 = Matrices.dense(numberCategory1, numberCategory2, pColumn)
		  val matrix2Transposed = matrix2.transpose
		  val rows21 = matrixToRddVector(matrix2Transposed, sc)
		  // Y = matrice des profils-colonnes
		  val matrixColumn = new RowMatrix(rows21)

		  // On applique ACP pour profile-ligne
		  val pcLine = matrixLine.computePrincipalComponents(2)
		  // On obtiendra les coordonnées des variables dans les lignes
		  val projectedLine = mat1.multiply(pcLine).rows.map(l => l.toArray).collect()

		  // Pour profile-colonne
		  val pcCol = matrixColumn.computePrincipalComponents(2)
		  val tableContingence = new RowMatrix(matrixToRddVector(matrixContinTransposed, sc))
		  // On obtiendra les coordonnées des variables dans les colonnes
		  val projectedCol = tableContingence.multiply(pcCol).rows.map(l => l.toArray).collect()

		  if (profil == "colonne"){return projectedCol} else {return projectedLine}  
		}
	}
}

