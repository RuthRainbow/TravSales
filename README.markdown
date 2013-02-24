Project forked from: 

***
TravSales
=========

A parallelized genetic algorithm attack on the Traveling Salesman problem. See [my related blog post](http://www.kickasslabs.com/2011/10/10/traveling-salesman-attack/ "Traveling Salesman Attack").

License
-------

This software is released under the [Apache License, version 2.0](http://www.apache.org/licenses/LICENSE-2.0 "Apache License 2.0").

Notes
-----

See the TODO file in this directory for a sort of sketchy roadmap of intended improvements.
***

My plan is to convert this into a hierarchical genetic algorithm. At the moment the implementation involves two jobs chained together.

Command line parameters:
-numCities or -c : set the number of cities
-populationSize or -s : set the population size (must be a multiple of 10)
-maxNumGenerations or -g : set the maximum number of generations the algorithm iterates over
-numHierarchyLevels or -h : set the number of hierarchy levels (TODO condition for this)
-filepath or -f : set the filepath for the results of each generation to be stored in as well as for the temporary files created by each hierarchy level
