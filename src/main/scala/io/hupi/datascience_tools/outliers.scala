/* Loading packages */
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.commons.math3.stat.descriptive._
import org.apache.commons.math3.distribution._

package io.hupi.datascience_tools {
 
  object Outliers {
  
    /* function : computeQuantileSorted(variable: RDD[Double], quantile: Double): Double
	 * This function compute quantile from a sorted Spark RDD
	 *
	 * @param variable: input data, it's a sorted RDD of Double 
	 * @param quantile: quantile to compute (eg. 85)
	 * @return value of input data at the specified quantile
	 */
	def computeQuantileSorted(variable: RDD[Double], quantile: Double): Double = {
	  val nb = variable.count
	  if (nb == 1) variable.first
	  else {
	    val n = (quantile / 100.0) * nb 
		val k = math.round(n).toLong
		val d = n - k
		if (k <= 0) variable.first()
		else {
		  val index = variable.zipWithIndex().map(_.swap)
		  if (k >= nb) {
			index.lookup(nb - 1).head
		  } else {
			index.lookup(k - 1).head // + d * (index.lookup(k).head - index.lookup(k - 1).head)
		  }
		}
	  }
	}
 
	/* function : computeQuantile(variable: RDD[Double], quantile: Double): Double
	 * This function compute quantile from an unsorted Spark RDD
	 *
	 * @param variable: input data, it's an unsorted RDD of Double 
	 * @param quantile: quantile to compute (eg. 85)
	 * @return value of input data at the specified quantile
	 */
	def computeQuantile(variable: RDD[Double], quantile: Double): Double = {
	  val rdd = variable.sortBy(x => x)
	  computeQuantileSorted(variable = rdd, quantile = quantile)
	}


	/* function : grubbs(variable : RDD[Double], alpha : Double = 0.05) : List[Double]
	 * This function try to detect some outliers in a feature with Grubbs'test for maximum and minimum
	 * So there is until 2 outliers (max and min) but if the test is negative for both then
	 * it returns an empty RDD.
	 * Grubbs test : H0 : max (or min) isn't an outlier / H1: max (or min) is an outlier
	 *
	 * @param variable: input data, it's a RDD of Double 
	 * @param alpha: test confidence (0.05 by default) between 0 and 1 (alpha in ]0,1[)
	 * @return a List[Double] with max or/and min if their test are rejected (so if there are outliers)
	 *         if both are not outlier, it returns an empty list.
	 */
	def grubbs(variable : RDD[Double], alpha : Double = 0.05) : List[Double] = {

	  // Settings control
	  if ( (alpha <= 0) || (alpha >= 1) ){
		throw new Exception("\"alpha\" needs to be strictly greater than 0 and strictly lower than 1.\n")
	  }
	  
	  // Compute some statistics
	  val N = variable.count
	  val mean = variable.sum / (N * 1.0)
	  val devs = variable.map( l => (l - mean) * (l - mean) )
	  val stddev = Math.sqrt( devs.sum / (N - 1.0) )
	  val mini = variable.min
	  val maxi = variable.max

	  // Test statistics values
	  val Gmax = Math.abs(maxi-mean) / stddev
	  val Gmin = Math.abs(mini-mean) / stddev
	  
	  // Criticals values under H0
	  val studentLaw = new TDistribution(N-2)
	  val tUni = Math.pow( studentLaw.inverseCumulativeProbability( alpha / N ) , 2 )
	  val GHUni = ( (N-1) / Math.sqrt(N) ) * Math.sqrt( tUni / (N-2+tUni) )
	  
	  // Return a list depending on the tests'results
	  if (Gmax > GHUni && Gmin > GHUni) { return List(maxi,mini) }
	  else if (Gmax > GHUni) { return List(maxi) }
	  else if (Gmin > GHUni) { return List(mini) }
	  else { return List() }

	}


	/* function : tukey(variable : RDD[Double], power : String = "strong") : RDD[Double]                           
	 * This function try to detect some outliers in a feature. It computes an interval called Tukey :
	 *   IQR (InterQuartile Range)= (Quartile 3)-(Quartile 1)
	 *   Interval = [(Quartile 1) – ( t × IQR) , (Quartile 3) + ( t × IQR)]
	 *   with t is the power (see below)
	 * If a value isn't in this interval, it's an outlier.
	 *
	 * @param variable: input data, it's a RDD of Double 
	 * @param power: degree of power of the interval
	 *               - "strong" : t = 3, it's for 'big' outliers
	 *               - "low" : t = 1.5, it's for all outliers
	 * @return a RDD[Double] which contains the outliers
	 */
	def tukey(variable : RDD[Double], power : String = "strong") : RDD[Double] = {

	  // Compute quartiles
	  val variableSorted = variable.sortBy(x => x)
	  val q1 = computeQuantileSorted(variableSorted,25)
	  val q3 = computeQuantileSorted(variableSorted,75)
	  
	  // Compute interval
	  val interval  = power.toLowerCase match {
		case "low" => ( q1 - 1.5 * (q3 - q1) , q1 + 1.5 * (q3 - q1) )
		case "strong" => ( q1 - 3 * (q3 - q1) , q1 + 3 * (q3 - q1) )
		case _ => throw new Exception("There are only 2 possibles choices for power : \"strong\" / \"low\".\n")
	  }
	  
	  // Filter to collect outliers
	  val outliers = variable.filter( l => ( (l < interval._1) || (l > interval._2) ) )
	  return outliers
	  
	}



	/* function : outliers3S(variable : RDD[Double], power : String = "strong") : RDD[Double]                           
	 * This function try to detect some outliers in a feature. It computes an interval (3 sigmas) :
	 *   m (moyenne) +/- t sigma (écart-type) with t is the power (see below)
	 * If a value isn't in this interval, it's an outlier.
	 *
	 * @param variable: input data, it's a RDD of Double 
	 * @param power: degree of power of the interval
	 *               - "strong" : t = 3,  it's for all outliers
	 *               - "low" : t = 4, it's for 'big' outliers
	 * @return a RDD[Double] which contains the outliers
	 */
	def out3S(variable : RDD[Double], power : String = "strong") : RDD[Double] = {

	  // Compute mean and standard deviation
	  val N = variable.count
	  val mean = variable.sum / (N * 1.0)
	  val devs = variable.map( l => (l - mean) * (l - mean) )
	  val stddev = Math.sqrt( devs.sum / (N - 1.0) )
	  
	  // Compute interval
	  val interval  = power.toLowerCase match {
		case "low" => ( mean-3*stddev , mean+3*stddev )
		case "strong" => ( mean-4*stddev , mean+4*stddev )
		case _ => throw new Exception("There are only 2 possibles choices for power : \"strong\" / \"low\".\n")
	  }
	  
	  // Filter to collect outliers
	  val outliers = variable.filter( l => ( (l < interval._1) || (l > interval._2) ) )
	  return outliers

	}


	/* function : esd(variable : RDD[Double], nbOutliers : Int = 10, alpha : Double = 0.05) : List[Double]    
	 * This function try to detect some outliers in a feature 
	 * with ESD's test (Extreme Studentized Deviate) generalized
	 * H0 : no outliers / H1 : There is until r outliers
	 * This function do the test for r in 1, 2, ... , nbOutliers and return all outliers for the
	 * biggest number for which the test is rejected.
	 *
	 * @param variable: input data, it's a RDD of Double 
	 * @param nbOutliers: number of test to do (and point)
	 * @param alpha: test confidence (0.05 by default) between 0 and 1 (alpha in ]0,1[)
	 * @return a RDD[Double] with all points considered as outliers
	 */
	def esd(variable : RDD[Double], nbOutliers : Int = 10, alpha : Double = 0.05) : List[Double] = {

	  val N = variable.count
	  val Ndistinct = variable.distinct.count
	  // Settings control
	  if ( (nbOutliers <= 0) || (nbOutliers >= Ndistinct) ){
		throw new Exception("\"nbOutliers\" needs to be strictly greater than 0 and strictly lower than " + Ndistinct + " (distinct values number).\n")
	  }
	  if ( (alpha <= 0) || (alpha >= 1) ){
		throw new Exception("\"alpha\" needs to be strictly greater than 0 and strictly lower than 1.\n")
	  }

	  // Initialisation of the number of outliers and the total list of outliers 
	  var giveNumberOutliers = 0
	  var listeOutliers : List[(Double,Double)] = List()

	  for ( i <- 1 to  nbOutliers) { 
		
		// Criticals values under H0 at step i
		val studentLaw = new TDistribution(N-i-1)
		val t = studentLaw.inverseCumulativeProbability(1-(alpha / (2*(N-i+1))))
		val lambda = ((N-i)*t) / Math.sqrt( (N-i-1 + Math.pow(t,2))*(N-i+1) )

		// Variable Without the i first Outliers
		val variableWO = variable.filter( l => !listeOutliers.map(_._2).contains(l) )
		
		// Compute mean and standard deviation
		val Ni = N - i + 1
		val moyenne = variableWO.sum / (Ni * 1.0)
		val square = variableWO.map( l => (l - moyenne) * (l - moyenne) )
		val std = Math.sqrt( square.sum / (Ni - 1.0) )
		
		// Test statistics values
		val variableStepZi = variableWO.map( l=> ( Math.abs(l-moyenne) / std , l) )
		val pointRemove = variableStepZi.max
		
		// Add to the list of outliers
		listeOutliers = listeOutliers ++ List(pointRemove)
		
		// If the test at step i is rejected then we update the number of outliers to take
		if (pointRemove._1 > lambda) giveNumberOutliers = i

	  }
	  
	  // Conversion in RDD of our selection of outliers
	  val outliersReal =  listeOutliers.map(_._2).take(giveNumberOutliers)
	  return (outliersReal)
	  
	}

	/* function : detection_value(variable : RDD[Double], method : String = "out3S", power : String = "strong"): RDD[Double]    
	 * This function try to detect some outliers in a feature with one of the four method below :
	 *   - out3S
	 *   - tukey
	 * See the description of each function for more information
	 *
	 * @param variable: input data, it's a RDD of Double 
	 * @param method: one of the two method available : 3S, Tukey
	 * @param power: degree of power of the interval for Tukey and 3S
	 *                - "Strong"-> all outliers
	 *                - "low" -> only for 'big' outliers
	 * @return a RDD[Double] with all points considered as outliers
	 */
	def detection_value ( variable : RDD[Double] , method : String = "3s" , power : String = "strong" ): RDD[Double] = {
		
	  method.toLowerCase match{
		case "3s" => out3S(variable, power)
		case "tukey" => tukey(variable, power)
		case _ => throw new Exception("\"method\" has to be \"3S\" or \"Tukey\" \n")
	  }
	}

	/* function : detection_test(variable : RDD[Double], method : String = "esd", alpha : Double = 0.05, nbOutliers : Int = 10): RDD[Double]
	 * This function try to detect some outliers in a feature with one of the 2 method below :
	 *   - esd
	 *   - grubbs
	 * See the description of each function for more information
	 *
	 * @param variable: input data, it's a RDD of Double 
	 * @param method: one of the 2 method available : ESD or Tukey
	 * @param alpha: test confidence (0.05 by default) between 0 and 1 (alpha in ]0,1[) for ESD 
	 * @param nbOutliers: number of test to do (and point) for ESD or grubbsma
	 * @return a List[Double] with all points considered as outliers
	 */
	def detection_test (
	  variable : RDD[Double],
	  method : String = "esd",
	  alpha : Double = 0.05,
	  nbOutliers : Int = 10
	  ): List[Double] = {
		
	  method.toLowerCase match{
		case "esd" => esd(variable, nbOutliers, alpha )
		case "grubbs" => grubbs(variable, alpha)
		case _ => throw new Exception("\"method\" has to be one of : ESD, 3S, Tukey or Grubbs \n")
	  }
	}
	

	/* function : replaceOutliersMean(variable: RDD[Double], outliers : RDD[Double]) : RDD[Double]
	 * This function replace the outliers by the mean of the variable
	 *
	 * @param variable: input data, it's a RDD of Double 
	 * @param outliers: input outliers, it's a RDD of Double which contains the outliers to replace
	 * @return a RDD[Double] which is the variable without outliers
	 */
	def replaceOutliersMean(variable: RDD[Double], outliers : RDD[Double]) : RDD[Double] = {
	  val outliersListe = outliers.collect
	  val N = variable.count
	  val mean = variable.sum / (N * 1.0)
	  return variable.map( l => if (outliersListe.contains(l)) mean else l )
	}

	/* function : replaceOutliersMedian(variable: RDD[Double], outliers : RDD[Double]) : RDD[Double]
	 * This function replace the outliers by the median of the variable
	 *
	 * @param variable: input data, it's a RDD of Double 
	 * @param outliers: input outliers, it's a RDD of Double which contains the outliers to replace
	 * @return a RDD[Double] which is the variable without outliers
	 */
	def replaceOutliersMedian(variable: RDD[Double], outliers : RDD[Double]) : RDD[Double] = {
	  val outliersListe = outliers.collect
	  val mediane = computeQuantile(variable,50)
	  return variable.map( l => if (outliersListe.contains(l)) mediane else l )
	}

	/* function : replaceOutliersQuartile(variable: RDD[Double], outliers : RDD[Double]) : RDD[Double]
	 * This function replace the outliers by the closest quartile of the variable
	 *
	 * @param variable: input data, it's a RDD of Double 
	 * @param outliers: input outliers, it's a RDD of Double which contains the outliers to replace
	 * @return a RDD[Double] which is the variable without outliers
	 */
	def replaceOutliersQuartile(variable: RDD[Double], outliers : RDD[Double]) : RDD[Double] = {
	  val outliersListe = outliers.collect
	  val variableSorted = variable.sortBy(x => x)
	  val q1 = computeQuantileSorted(variableSorted,25)
	  val q3 = computeQuantileSorted(variableSorted,75)
	  val varOutReplace = variable.map{
		l => if ( outliersListe.contains(l) ) {
			   if ( Math.abs(l-q3) > Math.abs(l-q1) ) q3 else q1
			 } else l
	  }
	  return varOutReplace
	}

	/* function : replaceOutliersMax(variable: RDD[Double], outliers : RDD[Double]) : RDD[Double] 
	 * This function replace the outliers by the maximum or minimum of the variable
	 *
	 * @param variable: input data, it's a RDD of Double 
	 * @param outliers: input outliers, it's a RDD of Double which contains the outliers to replace
	 * @return a RDD[Double] which is the variable without outliers
	 */
	def replaceOutliersMax(variable: RDD[Double], outliers : RDD[Double]) : RDD[Double] = {
	  val outliersListe = outliers.collect
	  val varWithoutOut = variable.subtract(outliers)
	  val maxi = varWithoutOut.max
	  val mini = varWithoutOut.min
	  val varOutReplace = variable.map{
		l => if ( outliersListe.contains(l) ) {
			   if ( Math.abs(l-maxi) <= Math.abs(l-mini) ) maxi else mini
			 } else l
	  }
	  return varOutReplace
	}


	/* function : replace(variable: RDD[Double], outliers : RDD[Double], replace: String = "nothing" ) : RDD[Double] 
	 * This function replace the outliers with one chosen value in "replace"
	 *
	 * @param variable: input data, it's a RDD of Double 
	 * @param outliers: input outliers, it's a RDD of Double which contains the outliers to replace
	 * @param replace: name method for replacement : "nothing"--> drop outliers
	 *   "mean" / "median" / "quartile" / "max" --> replace by the mean, the median, ...
	 *   See the replaceOutliers+name functions for more informations (e.g replaceOutliersMean)
	 * @return a RDD[Double] which is the variable treat with the method chosen
	 */
	def replace(variable: RDD[Double], outliers : RDD[Double], replace: String = "nothing" ) : RDD[Double] = {
	  
	  replace.toLowerCase match {
		case "nothing" => variable.subtract(outliers)
		case "mean" => replaceOutliersMean(variable, outliers)
		case "median" => replaceOutliersMedian(variable, outliers)
		case "quartile" => replaceOutliersQuartile(variable, outliers)
		case "max" => replaceOutliersMax(variable, outliers)
		case _ => throw new Exception("\"replace\" has to be one of : nothing, mean, median, quartile, max \n")
	  }

	}
  }
}