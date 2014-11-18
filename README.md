Spark Installation Instructions
===============================

1) Download spark from here: http://spark.apache.org/downloads.html 
hadoop1 is fine

2) Set SPARK_HOME and modify PYTHONPATH environment variables.
ex:
export SPARK_HOME=/path/to/spark-1.0.2-bin-hadoop1/
export PYTHONPATH=/path/to/spark-1.0.2-bin-hadoop1/python/:$PYTHONPATH

It's best to put these commands in something like .bashrc or .profile so that you don't have to remember them.

3) pip install py4j
there may be other dependencies too, if it breaks when run it should say why.

4) (optional) cp $SPARK_HOME/conf/log4j.properties.template $SPARK_HOME/conf/log4j.properties and modify log4j.rootCategory to be ERROR instead of INFO so that spark doesn’t print so much stuff

5) open up a python file or use the python console / ipython and start playing around:
    from pyspark import *
    sc = SparkContext("local")

    rdd = sc.parallelize([1,2,3])
    print rdd.count() #3

    doubled = rdd.map(lambda x: x*2)
    print doubled.collect() # [2, 4, 6]

    divided = doubled.flatMap(lambda x: [x/2.0, x/2.0])
    print divided.collect() # [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]

    filtered = divided.filter(lambda x: x > 1)
    print filtered.collect()

    text = sc.textFile("some_file.txt")
    print text.count() # number of lines in file

    print text.first() # first line of file

    print text.take(5) # first 5 lines of file
    
6) consult http://spark.apache.org/docs/latest/programming-guide.html specifically the (python) transformations and actions sections.
