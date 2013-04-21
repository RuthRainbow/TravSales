#TravSales
=========

Forked from BradHeintz's parallelised genetic algorithm attack on the Traveling Salesman problem. See [Brad's related blog post](http://www.kickasslabs.com/2011/10/10/traveling-salesman-attack/ "Traveling Salesman Attack").

The project has been converted into a hierarchical genetic algorithm with migration. The implementation starts from a single job, chaining a configurable number of hierarchical jobs.

The algorithm has also been generalised - the classes in the generalalgorithm package can be implemented with a problem other than TSP. The implementation for TSP can be found in the travsales package.

##Requirements
-------
- Java 7
- Maven
- Hadoop

##To compile
-------
From the project directory run 'mvn clean compile assembly:single'. This will create a single jar file in the target directory.

##To run
-------
Start your Hadoop cluster, then run './hadoop -jar projectdirectory/target/TravSales...jar'. 

As the algorithm runs output for each generation will be sent to standard out and written to the file travsales_populations/result. The final best chromosome will be written to the file travsales/finalresult.

##Command line parameters
-----
-numCities or -c : set the number of cities.  
-populationSize or -ps : set the population size (must be a multiple of 10).  
-maxNumGenerations or -g : set the maximum number of generations the algorithm iterates over.  
-numHierarchyLevels or -h : set the number of hierarchy levels.
-filepath or -f : set the filepath for the results of each generation to be stored in as well as for the temporary files created by each hierarchy level.  
-mutationChance or -mc : set the chance of mutation in the range [0.0, 1.0]. Must be a float.
-migrationRate or -mr : change the rate of migration, i.e. epoch length.
-migrationPercentage or -mp : change the percentage of individuals that migrate. Must be a float in the range [0.0, 1.0].
-survivorProportion or -sp : chance the proportion of individuals that survive each iteration. Must be a float in the range [0.0, 1.0].

##License
-------

This software is released under the [Apache License, version 2.0](http://www.apache.org/licenses/LICENSE-2.0 "Apache License 2.0").
