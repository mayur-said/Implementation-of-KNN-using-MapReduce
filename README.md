# Implementation-of-KNN-using-MapReduce
 ## Introduction
 K-nearest neighbors(KNN), a non-parametric lazy learning technique, is considered one of the best techniques for classification. The biggest advantage of KNN is that it does not make any assumptions about the data. This makes the KNN suitable for any practical data which generally doesn’t tend to follow any theoretical assumptions. However, on the downside, KNN has heavy computational demands and thus it can be difficult for a single machine to implement KNN on a large dataset. One of the ways to tackle this computational demand is by implementing KNN using distributed computation. Therefore, in this research project, KNN is implemented using the MapReduce programming model to predict customer satisfaction.

 To implement KNN using MapReduce, we have used the mrjob library in python. mrjob is one of the easiest ways to write python programs that run on Hadoop. 

 ## Steps to run this project
 1. Run the code in the jupyter notebook to pre-process the data. After pre-processing the dataset, the code in the notebook will divide the dataset into three parts: training, validation, and testing, and save them to the working directory as CSV files. 
 2. Run the knn_runner_script.py using the command "python knn_runner_script.py -r local --minmax ./minmax.txt --Max_K 10 --test ./test.csv ./train.csv". 
 3. After running the above script, we will get the accuracy, precision, and recall for each k value from 1 to Max_K. 

