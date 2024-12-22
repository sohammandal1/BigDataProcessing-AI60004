## Importing the libraries
import threading
from threading import Thread
import numpy as np
from queue import PriorityQueue
import sys
from scipy.spatial.distance import cosine

## Defining necessary functions
# Function for reading the files
def reading_files(file):
        temp=[]
        with open(file,'r') as file:
            for line in file:
                query_vector=list(map(float,line.split()))
                temp.append(query_vector)
        return temp

# Functions for calculation of cosine similarity
def cosineSimilarity(item_vector,query_vector):
    return( 1-cosine(item_vector[1:],query_vector))

# Function to implement multithreading
def multiThreading(item_vector,query_vector,start,end,results,id):
    for i in range(start,end):
        temp=cosineSimilarity(item_vector[i],query_vector[0])
        results[id].put((temp,item_vector[i][0]))
        if results[id].qsize()>k:
            results[id].get()

## Main function definition
if __name__=="__main__":
    if len(sys.argv)!=5:
        print("Please enter proper arguments")
        sys.exit(1)

    data_file=sys.argv[1]  
    query_file=sys.argv[2]
    num_threads=int(sys.argv[3])
    k=int(sys.argv[4])

    # Initiating multithreads
    item_vector=reading_files(data_file)
    query_vector=reading_files(query_file)
    
    threads=[None]*num_threads
    results=[PriorityQueue() for _ in range(num_threads)]

    size=len(item_vector)//num_threads
    
    # Thread starting
    for i in range(len(threads)):
        start=i*size
        end=(i+1)*size
        threads[i]=Thread(target=multiThreading,args=(item_vector,query_vector,start,end,results,i))
        threads[i].start()
    
    # Waiting until all threads join
    for i in range(len(threads)):
        threads[i].join()

    # Defining required priority queues
    ascending = PriorityQueue()
    elements = []
    
    for i in range(num_threads):
        while not results[i].empty():
            element=results[i].get()
            ascending.put(element)
            if ascending.qsize()>k:
                ascending.get()
    
    # Reversing the priority queue
    while not ascending.empty():
        elements.append(ascending.get())

## Printing the results
    for item in reversed(elements):
        print(int(item[1]), item[0])

## Use command line argument ###### python <your-code.py>  <data file>  <query item file>   <# threads>  <value of k> ######