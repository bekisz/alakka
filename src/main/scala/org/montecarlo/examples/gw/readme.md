# Galton-Watson Experiment 

A [Monte-Carlo simulation](https://en.wikipedia.org/wiki/Monte_Carlo_method)  of  the [Galton-Watson process](https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process) in [Scala](https://www.scala-lang.org/) on [Spark](https://spark.apache.org/)

The [Galton-Watson process](https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process) is the cornerstone of many genetic studies and has been extensively studied in several scholarly articles. Mostly by mathematical means. This scala code takes another approach and can run thousands and millions of [Monte-Carlo simulations](https://en.wikipedia.org/wiki/Monte_Carlo_method) to estimate the survival probabilities of lineages (seedNodes)  within a confidence interval with various lambda value (i.e. average number of descendants).

The advantage of the simulation approach is that simulations can easily be extended with lot more complex models, while mathematical approach has significant limitation when increasing the complexity of the model.

Enjoy the experimentation.


## Usage

Clone [Alakka repository](https://github.com/bekisz/alakka) from GitHub 
> git clone https://github.com/bekisz/alakka.git 

### Compiling and running
#### In Spark local mode
 
 In the root of the repository run :

> sbt -J-Dspark.master=local[*] "runMain org.montecarlo.examples.gw.GwExperiment 100" 

The value `100` sepcifies how many trials per each lambda (i.e all possible distinct input to trials) the experiment should make. The more this number, the longer the experiment lasts, but yielding more precise values for the probability of survival. 

#### On a Spark Cluster 
 
Running millions of simulations can become computation intensive that can hardly be carried out by the few cores of your laptop. Luckily Monte Carlo / Multiverse  makes it possible to lunch our experiment on a remote Spark cluster as well.
  
First, of course, you need to set up a [Spark]((https://spark.apache.org/)) cluster with [Livy](https://livy.apache.org/) as gateway.

Then edit upload-and-submit script file to reflect the parameters of your cluster at
`./src/main/script/upload-submit.sh` and in root directory run :

> sbt package && ./src/main/script/upload-submit.sh

### Tweaking the Experiment

TODO

## Architecture

### Package and Class Dependency

`* = org.montecarlo.examples.gw`
- *.__Experiment__
  - org.apache.__spark__ 
  - *.__GwInput__
    - *.__GwTrial__
      - *.__GwNode__
  - *.__GwOutput__
    - *.__GwTrial__
        - *.__GwNode__ 
  - *.__GwAnalyzer__
    - org.apache.__spark__ 
    - org.montecarlo.utils
      
Interpretation : The higher level entities (packages/classes/files) can see the lower level entities but not the other way around. Peers in the list are dependent from each other. For example, __GwTrial__ should reference neither  __GwInput__ nor __GwOutput__. A __Trial__ is independent of all other entities except __Node__.