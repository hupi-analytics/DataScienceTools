import org.apache.commons.lang3.StringUtils.stripAccents
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext

package io.hupi.datascience_tools {

	object SpellingError {
	
		// Fonction qui calcule le nombre de récurrences dans le dictionnaire
		def dicoWithNumbers(base : org.apache.spark.rdd.RDD[String]) : org.apache.spark.rdd.RDD[(String, Int)] = {
		  base.map(l => stripAccents(l).toLowerCase().replaceAll("[^A-Za-z]+", " ").filter(!_.isDigit))
			  .flatMap(line => line.split(" "))
			  .map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
		}

		// Fonction qui cherche le bon dictionnaire
		def findDictionary (secteur : String, sc: SparkContext) : org.apache.spark.rdd.RDD[(String, Int)] = secteur match {
		  case "droit" => dicoWithNumbers(sc.textFile("hdfs://hupi.node1.pro.hupi.loc/user/helene.nguyen/SpellingError/codeCivil.txt"))
		  case "economie" | "finance" => dicoWithNumbers(sc.textFile("hdfs://hupi.node1.pro.hupi.loc/user/helene.nguyen/SpellingError/projetLoiFinance2016.txt"))
		  case "informatique" => dicoWithNumbers(sc.textFile("hdfs://hupi.node1.pro.hupi.loc/user/helene.nguyen/SpellingError/introInformatique.txt"))
		  case "litterature" => dicoWithNumbers(sc.textFile("hdfs://hupi.node1.pro.hupi.loc/user/helene.nguyen/SpellingError/litteratureFrancaise.txt"))
		  case "sante" => dicoWithNumbers(sc.textFile("hdfs://hupi.node1.pro.hupi.loc/user/helene.nguyen/SpellingError/quotidien/sante.txt"))
		  case _ => dicoWithNumbers(sc.textFile("hdfs://hupi.node1.pro.hupi.loc/user/helene.nguyen/SpellingError/quotidien/Propre.csv"))
		}

		val alphabet = "abcdefghijklmnopqrstuvwxyz"
		val n = alphabet.length

		// Détection en distance 1 (les possibilités d'erreurs de frappe les plus récurrentes)
		// --1/ Quand on a oublié de taper un caractère dans le mot
		// --2/ Quand on a changé la place des caractères avec des caractères "voisins"
		// --3/ Quand on a remplacé un caractère par un autre 
		// --4/ Quand on ajoute un nouveau caractère dans le mot
		// Pour un mot de n caractères, il y aura donc n mots pour 1/, n-1 pour 2/, 26n pour 3/, et 26(n+1) pour 4/
		// Du coup on aura en total une liste de 54n+25 mots (avec répétitions)

		def wordsTransform (mot : String) : List[String] = {
		  // Splits
		  val split = new ListBuffer[List[String]]()
		  for (i <- 0 to mot.length){
			split += List(mot.substring(0, i), mot.substring(i))
		  }

		  // Si on efface un caractère dans le mot
		  val del = new ListBuffer[String]()
		  del += mot.substring(1) += mot.substring(0, mot.length-1)
		  for (i <- 1 to (mot.length-2)) {
			del += mot.substring(0,i)+mot.substring(i+1)
		  }

		  // Si on change la place des caractères
		  val transpose = new ListBuffer[String]()
		  val split1 = split.filter(l => l(1).length > 1)
		  for (i <- 0 to (split1.length-1)) {
			transpose += split1.map(l => l(0)).toList(i) + split1.map(l => l(1)).toList(i)(1) + split1
							   .map(l => l(1)).toList(i)(0) + split1.map(l => l(1)).toList(i).substring(2)
		  }

		  // Si on remplace des caractères
		  val replace = new ListBuffer[String]()
		  val split2 = split.filter(l => l(1).length > 0)
		  for (i <- 0 to (split2.length-1)) {
			for (j <- 0 to (n-1)) {
			  replace += split2.map(l => l(0)).toList(i) + alphabet(j) + split2.map(l => l(1)).toList(i).substring(1)
			}
		  }

		  // Si on ajoute un nouveau caractère
		  val insert = new ListBuffer[String]()
		  for (i <- 0 to (split.length-1)) {
			for (j <- 0 to (n-1)) {
			  insert += split.map(l => l(0)).toList(i) + alphabet(j) + split.map(l => l(1)).toList(i)
			}
		  }

		  // On combine toutes les cas possibles dans une liste
		  val liste = (del ++ transpose ++ replace ++ insert).toList
		  return liste
		}

		// Détection en distance 2 (autrement dit, c'est la double détection de distance 1)
		// On cherche les erreurs possibles de distance 1 pour un mot, puis pour chaque mot dans la nouvelle liste,
		// on cherche encore ses erreurs possibles dans distance 1

		def findWords (mot : String, secteur : String = "quotidien", sc : SparkContext) : List[String] = {
		  val dico = findDictionary(secteur, sc)
		  val words= wordsTransform(mot).distinct
		  val newList = new ListBuffer[List[String]]()
		  for (i <- 0 to (words.length-1)) {
			val k = wordsTransform(words(i))
			newList += k
		  }
		  val newList1 = newList.toList.flatten.distinct
		  // On cherche les mots qui sont dans le dictionnaire
		  val j = List(newList1, dico.map(l => l._1).collect().toList)
		  val finalList = j.reduceLeft(_.intersect(_))
		  return finalList
		}

		// Fonction qui supprime les accents, les points d'exclamation, les chiffres dans word et on le transforme en un Array des caractères
		// puis supprime les caractères répétés plusieurs fois dans le mot
		def neatWord (word : String) : String = {
		  val input = stripAccents(word).toLowerCase().replaceAll("[^A-Za-z]+", "")
										.filter(!_.isDigit).split("").filter(l => l != "")
		  val newList = new ListBuffer[String]()
		  var i = 0
		  while (i < input.length-1) {
			if (input(i+1) != input(i)) {
			  newList += input(i)
			  i = i+1
			} else {
			  i = i+1
			}
		  }
		  val newWord = (newList += input.last).toList.mkString("")
		  return newWord
		}

		// Fonction qui détermine la liste des erreurs de frappe		
		val listeError = new ListBuffer[String]()
		def errorList (input : Array[String], secteur : String = "quotidien", sc : SparkContext) : List[String] = {
		  val dico = findDictionary(secteur, sc) 
		  val words = dico.map(l => l._1).collect()
		  for (i <- 0 to input.length-1) {
			if (!words.contains(input(i))) {
			  listeError += input(i)
			}
		  }
		  return listeError.toList
		}

		// Fonction qui selectionne les mots les plus probables
		// L'algorithme est donc de regarder pour chaque mot entré, 
		// --> s'il est dans le dictionnaire => la probabilité qu'il est bon est la plus haute (évident).
		// --> on descend, mtn si la listEff (liste des mots qui sont causés par des erreurs de distance 1) n'est pas nulle 
		//      => on prend le mot avec la probabilité d'occurrence la plus haute"
		// --> Si la listeEff est nulle et que la listeEff2 n'est pas nulle => meme logique, on prendra le mot qui apparait le plus souvent
		// --> Si la listeEff2 est aussi nulle => le mot est très mal écrit et on ne peut trouver sa version correcte.


		def correctWord(word : String, secteur : String = "quotidien", sc : SparkContext) : String = {
		  // On cherche le bon dictionnaire
		  val dico = findDictionary(secteur, sc)
		  val words = dico.map(l => l._1).collect()
		  // On applique la fonction de traitement de mot
		  val mot = neatWord(word)
		  
		  if (words.contains(mot)) {
			return mot
		  } else {
			val m1 = wordsTransform(mot)
			val m2 = new ListBuffer[String] ()
			for (i <- 0 to (m1.length - 1)) {
			  if (words.contains(m1(i))){
				m2 += m1(i)
			  } else {
				m2 += "not existed"
			  }
			}
			val motTransforme1 = m2.filter(l => l != "not existed")
			val motTransforme2 = findWords(mot, secteur, sc)
			
			val listeEff1 = new ListBuffer[Array[(String, Int)]]()
			for (i <- 0 to motTransforme1.length - 1) {
			  val effectif = dico.filter(l => l._1 == motTransforme1(i)).map(l => (l._1, l._2)).collect()
			  listeEff1 += effectif
			}
			val listeEff2 = new ListBuffer[Array[(String, Int)]]()
			for (i <- 0 to motTransforme2.length - 1) {
			  val effectif = dico.filter(l => l._1 == motTransforme2(i)).map(l => (l._1, l._2)).collect()
			  listeEff2 += effectif
			}
			if (listeEff1.length != 0) {
			  val res1 = listeEff1.toList.map(l => l.toMap).flatten.distinct.sortBy(_._2).map(l => l._1).last
			  return res1
			} else if (listeEff2.length != 0) {
			  val res2 = listeEff2.toList.map(l => l.toMap).flatten.distinct.sortBy(_._2).map(l => l._1).last
			  return res2
			} else {
			  return mot
			  //return "Le dictionnaire ne comprend pas le mot (causes possibles : différente langue, pas le bon dictionnaire,  
			  //capacité de dictionnaire ou le mot est utilisé rarement)"
			}
		  }
		}

		// Fonction pour corriger un phrase
		def correctSentence (input : String, secteur : String = "quotidien", sc:SparkContext) : String = {
		  val dico = findDictionary(secteur, sc)
		  val words = input.split(" ")
		  val goodWords = new ListBuffer [String]()
		  for (i <- 0 to (words.length - 1)) {
			val w = neatWord(words(i))
			val res = correctWord(w, secteur, sc)
			goodWords += res
		  }
		  val textPropre = goodWords.mkString(" ")
		  return textPropre
		}
	}
}
