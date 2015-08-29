### Amazon-Dynamo-Based-Key-Value-Storage  
____________________________________


A Simplified version of Amazon Dynamo for key value storage

Amazon Dynamo based  key value storage provides 3 main functionalities:- 1) Partitioning 2) Replication 3) Failure Handling

1.  Partitioning:- Partitioning provides load balancing among the various nodes present in the system. SHA-1 cryptographic hash function is used to partition the key value pairs using lexicographical order. This is similar to CHORD with the little difference, that each and every node in the system knows every other node. Due to this property of the system, key value pair can be sent to the appropriate node directly, which further ensures the "Membership" as the main sending node will be waiting for the acknowledgment from the coordinator.

2. Replication:- Chain replication is used to ensure persistant storage of key value pairs among the appropriate nodes. Key value pair is sent to the main coordinator for that pair and that coordinator makes sure that it is replicated to its 2 successive nodes. This method of replication provides "Linearizability" as the write operation is always routed to the head of the chain and the read operation is always routed to the tail of the chain.

3.  Failure Handling:- In case of failure of any node in the system, the request for the key value pair is sent to its corresponding successor, which provides with the result. When the node recovers from the failure, all the missed key value pairs for that node are copied from the corresponding successor and predecessor. Socket read timeout is used to detect the failure of node in the system.

Complete description of the project can be read from [here](https://docs.google.com/document/d/1iHtWvSE2pul7_OhcUMkZ9EMGn1IFhhPwwmz00TDLjP8/edit)