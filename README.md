# Big-Data-Project-Link-Prediction

Instructions to run the code:

1. First download the dataset from the following link:
http://times.cs.uiuc.edu/~wang296/Data/

2. Select the first dataset on the list i.e TripAdvisor Dataset and download it in json format.

3. Unzip the downloaded link and place the folder named "json" in the code directory.

4. Now create the following directory structure inside the code directory. The program files will process data and dump the intermediate and the final results in json files 
which will be stored in specific directories.

5. Create a folder named "data". After creating it, inside the data folder create four folders with the following names:
	a) train
	b) test
	c) created
	d) stats

6. Once the folder structure has been created, first run the reviewExtractor.py file.

7. Once the execution completes, next run the pre_data_processing.py file.

8. After that completes run the graph_creator.py file followed by matrix_creator.py file.

9. Once these programs have been completed, you can then run the similarity, random walks and svd code. 

10. After running all of them then you can run the binary classification code.	