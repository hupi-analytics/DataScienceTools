// Loading package
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.commons.math3.stat.descriptive._
import org.apache.commons.math3.distribution._

package io.hupi.datascience_tools {

	object Discretisation {
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
			//val d = n - k
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

		/* foundDiscretisationClasse(variable : RDD[Double], method : String = "scott") : Int 
		 * This function try to find the number of discretisation with one of the ficve method below
		 *
		 * @param variable: input data, it's an RDD of Double 
		 * @param method: mathematic formula for determining the number of class :
		 *                 scott (default), brooks-carruthers, huntsberger, sturges, freedman-diaconis 
		 * @return the number given by the formula
		 */
		def foundDiscretisationClasse(variable : RDD[Double], method : String = "scott") : Int = {
		  
		  val N = variable.count

		  method.toLowerCase match {
			case "brooks-carruthers" => Math.round( 5.0 * Math.log10(N) ).toInt
			case "huntsberger" => Math.round( 1.0 + 3.332 * Math.log10(N) ).toInt
			case "sturges" => Math.round( Math.log(N+1) / Math.log(2) ).toInt
			case "scott" => {
			  val mean = variable.sum / (N * 1.0)
			  val devs = variable.map( l => (l - mean) * (l - mean) )
			  val stddev = Math.sqrt( devs.sum / (N - 1.0) )
			  val mini = variable.min
			  val maxi = variable.max
			  Math.round( (maxi-mini) / ( 3.5 * stddev * Math.pow(N, (-1/3)) ) ).toInt
			}
			case "freedman-diaconis" => {
			  val variableSorted = variable.sortBy(x => x)
			  val q1 = computeQuantileSorted(variable = variableSorted, quantile = 25)
			  val q3 = computeQuantileSorted(variable = variableSorted, quantile = 75)
			  val mini = variableSorted.min
			  val maxi = variableSorted.max
			  Math.round( (maxi-mini) / ( 2.0*(q3-q1)*Math.pow(N,(-1/3)) ) ).toInt
			}
			case _ => throw new Exception("Method name for determining the class number isn't correct. \n" +
					  "Please chose one in this list : brooks-carruthers, huntsberger, sturges, scott, freedman-diaconis \n")
		  }
		}


		/* function : discretisationQuantile(variable : RDD[Double], nbClass : Int) : List[Double] 
		 * This function discretized a quantitative variable into a given number of class
		 * It returns the list of bounds of the intervals. Intervals are calculated with the 
		 * same number of point in each interval (quantile)
		 *
		 * @param variable: input data, it's an RDD of Double 
		 * @param nbClass: the number of class (discretisation number)
		 * @return a list of bounds 
		 */
		def discretisationQuantile(variable : RDD[Double], nbClass : Int) : List[Double] = {
		  val div  = 100 / nbClass.toDouble
		  val variableSorted = variable.sortBy(x => x)
		  val bounds = ( 0 to nbClass ).toList.map( l => computeQuantileSorted(variableSorted, l*div) )
		  return bounds
		}


		/* function : discretisationAmplitude(variable : RDD[Double], nbClass : Int) : List[Double] 
		 * This function discretized a quantitative variable into a given number of class
		 * It returns the list of bounds of the intervals. Intervals are calculated with the 
		 * same distance(amplitude) between bounds 
		 *
		 * @param variable: input data, it's an RDD of Double 
		 * @param nbClass: the number of class (discretisation number)
		 * @return a list of bounds 
		 */
		def discretisationAmplitude(variable : RDD[Double], nbClass : Int) : List[Double] = {
		  val mini = variable.min
		  val div  = (variable.max - mini) / nbClass.toDouble  
		  val bounds = ( 0 to nbClass ).toList.map( l => mini+l*div )
		  return bounds
		}


		/* function : labelsDeterminationOption (
			labels : List[String], nbClass : Int, bounds : List[Double], direction : String ) : List[String]
		 * This function return a list in which we have the differents labels possible, 
		 * corresponding to the choice we made
		 *
		 * @param labels: list of labels : 
		 *                 - More than one label, example : List("Little","Big")
		 *                 - List("LETTERS") --> labels are letters A, B, ...
		 *                 - List("NUMBERS") --> labels are numbers 0, 1, ...
		 *                 - List("DEFAULT") --> labels are bounds in "[]" , for example : [ 0 ; 5 [ or [ 5 ; 10] 
		 * @param nbClass: the number of class (discretisation number)
		 * @param bounds: list of the intervals bounds
		 * @param direction: String representing a choice between "right" --> [x,y[ or "left" --> ]x,y]
		 * @return a list of bounds 
		 */
		def labelsDeterminationOption(labels : List[String], nbClass : Int, bounds : List[Double], direction : String) : List[String] = {
		  
		  labels match {
			case List("LETTERS") => ( 'A' to ('A'.toInt + (nbClass-1)).toChar ).toList.map( l => l.toString) 
			case List("NUMBERS") => ( 0 to (nbClass-1) ).toList.map( l => l.toString)
			case List("DEFAULT") => {
			  direction match {
				case "right" => ( 0 to (nbClass-2) ).toList.map( l => ( "[ "+bounds(l)+" ; "+bounds(l+1)+" [" ) ) ++ 
								List("["+bounds(nbClass-1)+" ; "+bounds(nbClass)+"]")
				case "left" => List("["+bounds(0)+" ; "+bounds(1)+"]") ++
							   ( 1 to (nbClass-1) ).toList.map( l => ( "[ "+bounds(l)+" ; "+bounds(l+1)+" [" ) )              
				case _ => throw new Exception ("The direction has to be among \"right\" [.,.) and \"left\" (.,.] . \n")
			  }
			}
			case _ => labels
		  }
		}


		/* function : discretisationRDDBounds( variable : RDD[Double],bounds : List[Double], labels : List[String],
		 *                                     direction : String = "right") : RDD[(Double,String)]
		 * This function discretized a variable, it uses a list of bounds to determined the class and then 
		 * it concatenates the corresponding label
		 *
		 * @param variable: input data, it's an unsorted RDD of Double
		 * @param bounds: list of the intervals bounds
		 * @param labels: list of labels 
		 * @param direction: String representing a choice between "right" --> [x,y[ or "left" --> ]x,y]
		 * @return a RDD with the value and the label associated
		 */
		def discretisationRDDBounds( variable : RDD[Double], bounds : List[Double], labels : List[String],direction : String = "right")
		  : RDD[(Double,String)] = {
		  
		  val N = variable.count
		  val nbClass = bounds.length - 1
		  
		  if ( nbClass < 1 ){
			throw new Exception ("Bounds : You have to give a list of Double with at least two numbers. \n")
		  }else if ( N == 0 ) {
			throw new Exception ("Your RDD \"variable\" is empty. \n")
		  }else if ( bounds.min > variable.min || bounds.max < variable.max) {
			throw new Exception ("Bounds have to contain all the data . \n")
		  }else if ( labels.length != nbClass ) {
			throw new Exception ("The number of class is different from the number of labels. \n")
		  }else{
			direction.toLowerCase match {
			  case "right" => variable.map{ l => { var value = labels(nbClass-1)
												   for(i <- 1 to nbClass){ 
													 if (bounds(i-1)<=l && l<bounds(i) ){ value = labels(i-1)} 
												   } 
												   (l,value) 
												 }
										  }
			  case "left" => variable.map{ l => { var value = labels(0)
												   for(i <- 1 to nbClass){ 
													 if (bounds(i-1)<l && l<=bounds(i) ){ value = labels(i-1)} 
												   } 
												   (l,value)  
												 }
										  }
			  case _ => throw new Exception ("The direction has to be among \"right\" [.,.) and \"left\" (.,.] . \n")
			}
		  }
		}



		/* Description de la fonction

		Cette fonction a pour but de discrétiser une variable continue. Elle possède 3 méthodes de discrétisation différentes ainsi que 
		plusieurs méthodes pour calculer automatiquement un nombre de classes. Elle retourne un RDD[(Double,String)] contenant la valeur 
		initiale et la classe associée.

		Les paramètres sont :
		- variable : Variable continue à discrétiser
		- methode : Nom de la méthode de discrétisation
					* quantile : découpe en classes d'effectifs égaux
					* amplitude : découpe en classes de longueurs égales (remarque : une classe peut être vide et donc ne pas apparaitre)
					* manuelle : découpe manuellement les données --> besoin de donner les différents intervalles dans "bornes"
		- methode_nb_classe : Nom de la méthode qui permet de déterminer le nombre de classes "automatiquement"
							  * Brooks-Carruthers
							  * Huntsberger
							  * Sturges
							  * Scott
							  * Freedman-Diaconis
		  Ce paramètre est inactif lorsque le nombre de classes est précisé ("nb_classe")
		- nb_classe : nombre de classes (0 si déterminé automatiquement)
		- labels : Liste contenant les noms des classes (ordonnés), il faut que ce nombre correspond au nombre de classes
				   Si ce champ n'est pas précisé, le label correspondra à "BorneInf - BorneSup".
				   Une solution possible est List("LETTERS") qui permet de remplacer les noms de variable par "A","B", ...
		- right : Choix entre intervalle [a,b[ (True) ou ]a,b] (False)
		- bornes : C'est un paramètre obligatoire pour la méthode "manuelle" car c'est elle qui détermine le découpage à effectuer.
				   C'est donc une liste avec l'ensemble des limites pour le découpage.
				   Exemple : bornes=List(0.0,3,6,10) découpe les données en 3 --> [0,3[, [3,6[ et de [6,10[
		*/



		/* function : discretisationRDDBounds( variable : RDD[Double],bounds : List[Double], labels : List[String],
		 *                                     direction : String = "right") : RDD[(Double,String)]
		 * This function discretized a variable quantitative. It can determined automatically the number of class (5 formulas).
		 * There is 3 method for determined who to split the variable and one is to give the intervals bounds in a list.
		 * We can associate differents labels to the discretized variable. 
		 *
		 * @param variable: input data, it's an RDD of Double
		 * @param method: The name of the method :
		 *                  - "Manual" --> Intervals are determined by the user in boundsInterval
		 *                  - "Quantile" --> Intervals are calculated with the same number of point in each interval (quantile) 
		 *                  - "Amplitude" --> Intervals are calculated with the same distance(amplitude) between bounds 
		 * @param methodNbClass: mathematic formula for determining the number of class :
		 *                        scott (default), brooks-carruthers, huntsberger, sturges, freedman-diaconis 
		 *                       If nbClasse is specified, this parameter is unheeded
		 * @param nbClasse: Number of class, if you want to use a method to determined the number of class then put None, else Some(nb)
		 * @param labelsNames: list of labels : 
		 *                       - More than one label, example : List("Little","Big")
		 *                       - List("LETTERS") --> labels are letters A, B, ...
		 *                       - List("NUMBERS") --> labels are numbers 0, 1, ...
		 *                       - List("DEFAULT") --> labels are bounds in "[]" , for example : [ 0 ; 5 [ or [ 5 ; 10] 
		 * @param direction: String representing a choice between "right" --> [x,y[ or "left" --> ]x,y]
		 * @param boundsInterval: if the method choosen is "manual" then specified the interval you want with a list
		 *                        which have at least two element (one class) which contains all points in variable.
		 *                        It has to correspond to the number of class if nbClasse is given. 
		 * @return a RDD with the value and the label associated RDD[(Double,String)]
		 */
		def discretisation (variable : RDD[Double], method : String = "quantile", methodNbClass : String = "scott",
							nbClasse : Option[Int] = None, labelsNames : List[String] = List("DEFAULT"),
							direction : String = "right", boundsInterval : List[Double] = List() ) : RDD[(Double,String)] = {
		  
		  val nlabels = labelsNames.length
		  val nbornes = boundsInterval.length
		  // Computation of the number of class
		  val nbClass = nbClasse.getOrElse{
			// Si le nombre de classe n'est pas précisé, on le calcule
			if(nlabels>1){ 
			  nlabels       // Si on a au moins 2 labels alors K est égale au nombre de labels
			}else if (method == "manual") {
			  // Si l'on veut découper manuellement les données alors le nombre de découpage se trouve dans bornes
			  (nbornes - 1)
			}else{               // Sinon on applique une formule de calcul
			  foundDiscretisationClasse(variable, methodNbClass)    
			}   
		  }
		  
		  // Gestion d'erreurs sur le nombre de labels
		  if(nlabels==0)
			throw new Exception ("The label list is empty, give at least 2 labels or just \"DEFAULT\", \"LETTERS\" or \"NUMBERS\". \n")
		  else if ( nlabels==1 && labelsNames.head != "LETTERS" && labelsNames.head != "NUMBERS" && labelsNames.head!="DEFAULT" )
			throw new Exception ("Please give a list of labels with at least 2 labels or just \"LETTERS\" or \"NUMBERS\". \n")
		  else if (nlabels > 1 && nbClass!= nlabels) 
			throw new Exception ("The class number ( = " + nbClass + ") and the label number ( = " + nlabels + ") aren't equal. \n")
		  
		  if (nbClass <= 0) throw new Exception ("The discretisation (class) number is lower or equal to 0. \n")
		  
		  val bounds = method.toLowerCase match {
			case "manual" => if (boundsInterval.length<2)
							   throw new Exception ("You have to precise the intervals bounds with at least 2 values. \n")
							 else boundsInterval.sorted
			case "quantile" => discretisationQuantile(variable, nbClass)
			case "amplitude" => discretisationAmplitude(variable, nbClass)
			case _ => throw new Exception ("The method has to be one of \"manual\", \"quantile\" or \"amplitude\". \n")
		  }
		  
		  val labels = labelsDeterminationOption(labelsNames, nbClass, bounds, direction)
		  
		  val vardiscretized = discretisationRDDBounds(variable, bounds, labels, direction)
		  
		  return vardiscretized 
		}
	}
}