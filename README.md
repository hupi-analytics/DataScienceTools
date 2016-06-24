# DataScienceTools
> Data qualification tools developped by [HUPI](http://www.hupi.fr/).

```
import io.hupi.datascience_tools._
```

## Introduction :
In this folder, you will find some tools for outliers detection, discretisation, correlation analysis and text correction.


## Description :

This package has (for the moment) 4 tools :

1. Outliers detection : We use some statistical test and/or some hypothesis to determine if there is outliers in the RDD. Then there is a function to replace this points with some features like mean, mediane, nearest quartile, nearest "maximum", ... 

  A short preview :
  ```
  Outliers.detection_test ( variable, method = "esd", alpha = 0.05, nbOutliers = 10 )
  Outliers.replace( variable, outliers, replace = "quartile" )
  ```

2. Discretisation : This function allows you to discretized a quantitative variable with many options.

  A short preview :
  ```
  val discretize_data = Discretisation.apply_on ( variable, method = "quantile", nbClasse = None, direction = "right", labelsNames = List("NUMBERS") )
  ```

3. Correlation : There is a lot of function on dataframe which compute some features about correlation. 

  A short preview :
  ```
  Correlation.findBestCor( data, schemaData, method = "pearson", test =  "fisher", alpha = 0.05, numberOfCouples = 5, direction = "positive", level = "most" )
  ```

4. Text correction : With this function you can directly correct your text, it mostly removes typing mistakes.

  A short preview :
  ```
  val texte = "Nous soummes des speciallistes du big-data."
  val correct = SpellingError.correctSentence (input = texte, secteur = "quotidien", sc)
  ```

The "Example.txt" file contains a short example for some of this function.


[HUPI](http://www.hupi.fr/) - Anthony LAFFOND and Minh-tu Nguyen
