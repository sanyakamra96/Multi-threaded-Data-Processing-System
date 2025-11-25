# Multi-threaded-Data-Processing-System
# Assignment 6: Multi-threaded Data Processing System (Java + Go)

## Overview
This repository contains two implementations of a multi-threaded Data Processing System:
1. **Java** using ExecutorService, BlockingQueue, and ReentrantLock
2. **Go** using goroutines, channels, and sync.Mutex

Each system launches multiple workers, processes tasks in parallel, logs lifecycle events, and writes results to a shared output file.

---

## Java Implementation

### How to Run
**Requirements:** JDK 17+ (or JDK 11+)

```bash
cd java
# if using Maven:
mvn clean package
mvn exec:java -Dexec.mainClass="com.example.DataProcessingSystem"
