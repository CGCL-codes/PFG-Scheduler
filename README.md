## Pre-filtering Grouping:

The pre-filtering grouping (PFG) dynamically monitors the items of a stream and greatly improves the accuracy of estimation by keeping the actual key-value pair for the frequent items. On one hand,
to ensure better load balancing for the skewed data streams, the detected hot keys are directed to more than two processing elements randomly from the limited workers. On the other hand, for less frequent keys, the proposed scheme explores the principle of the power of two choices to distribute load
##  Introduction:

With the rapid advancement and usage of Internet technologies, an increasingly large volume of data is generated daily with real-time processing requirements. In this context, DSPS have gained significant attention from both academia and industry due to the ability to process millions of records in milliseconds. Many enterprises and institutions are now using DSPS (e.g., Storm, Flink, and Spark Streaming) to carry out computations on a considerably large volume of streaming data. In a DSPS,a processing element (PE), also known as a task instance, uniquely identifies the source of data, processes it, and generates the output in a real time.
Identifying the frequent keys in a real-time data stream is the key to efficient data partitioning 11. Various hash-based probabilistic data structures (e.g., counting bloom filter (CBF) and count-min sketch (CMS)) have been employed for maintaining a synopsis for the incoming data streams. However, the accuracy and false-positive rate of existing designs depend on the structure’s length and hash functions. Several algorithms (e.g., augmented sketch (ASketch), cold filter (CF), and skimmed sketch) have been proposed to address challenges for synopsis construction. These schemes are based on the pre-filtering of hot items or cold items in a data stream. These designs either have a frequent exchange of items between these two layers due to the changes in keys popularity, leading to frequent memory access or more average relative error. 


To solve the aforementioned issue we propose a streaming algorithm and data partitioning strategy. Our pre-filtering approach consists of three layers. At the first layer, all data items pass through the initial filter, which keeps track of the actual items and their frequencies. The second layer consists of hash-based counters of the fixed size. Lastly, the third layer uses a small size count-min sketch, which is initiated when the second layer counters overflow. The PF considers both the actual keys and the size of counters to improve the accuracy of predicting hot items and reduces the miss-classification rate without using any additional space. Items are dynamically shifted between these layers depending on keys frequency. Small counters for cold items improves the memory efficiency and ensure proper utilization of allocated memory. The design of the study can be depicted through Figure 1.
## Design of Study :
![dataarchitecture2](https://user-images.githubusercontent.com/71701753/112271378-172fd500-8c38-11eb-8b1b-c4315ca2ed6b.png)



## Build the PFG:
1: PFG is build on the top of Apache Storm. Make a .Jar file of the source and add to the directory of Apache Storm source files:
## How to use: 

builder.setBolt("zipfseconf-bolt", new zipFCountBolt(),20).
                customGrouping("zipfist-bolt", new ProposedPartitioningScheme()).setNumTasks(50);
## Contact: 
If you feel any problem please feel free to contact at adeelaslam@hust.edu.cn
## Author and Copyright
PFG is developed in Big Data Technology and System Lab, Services Computing Technology and System Lab, Cluster and Grid Computing Lab, School of computer Science and Technology, Huazhong University of Science and Technology, Wuhan, China by Adeel Aslam (adeelaslam@hust.edu.cn), Hanhua Chen (chen@hust.edu.cn), and Hai Jin (hjin@hust.edu.cn)

Copyright(C) 2021, STCS & CGCL and Huazhong University of Science and Technology.







