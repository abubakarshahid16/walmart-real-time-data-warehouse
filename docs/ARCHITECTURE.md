# System Architecture

## Overview

This system implements a real-time data warehouse using the **Hybrid Join algorithm** for efficient ETL processing of streaming sales transactions.

## Architecture Diagram

```mermaid
graph TB
    subgraph "Data Sources"
        CSV1[Transactional Data<br/>550K records]
        CSV2[Product Master<br/>3.6K products]
        CSV3[Customer Master<br/>5.9K customers]
    end

    subgraph "ETL Layer"
        Producer[Producer Thread<br/>Stream Reader]
        Consumer[Consumer Thread<br/>Hybrid Join Engine]
        
        subgraph "Memory"
            HashTable[Hash Table<br/>10K slots]
            Queue[FIFO Queue]
            Cache[Dimension Cache]
        end
        
        subgraph "Disk Simulation"
            P1[Partition 1]
            P2[Partition 2]
            P3[...]
            P8[Partition 8]
        end
    end

    subgraph "Data Warehouse - BlackDW"
        DW[(MySQL Database)]
        
        subgraph "Star Schema"
            FC[FactSales<br/>550K records]
            DC[DimCustomer]
            DP[DimProduct]
            DS[DimStore]
            DSup[DimSupplier]
            DD[DimDate]
        end
    end

    subgraph "Presentation Layer"
        Dashboard[Streamlit Dashboard]
        Tab1[Real-Time Monitor]
        Tab2[OLAP Queries]
    end

    CSV1 -->|Stream| Producer
    CSV2 -->|Partition| P1 & P2 & P3 & P8
    CSV3 -->|Load| Cache
    
    Producer -->|Push| HashTable
    HashTable --> Queue
    Queue -->|Pop| Consumer
    P1 & P2 & P3 & P8 -->|Load| Consumer
    Cache -->|Lookup| Consumer
    
    Consumer -->|Insert| FC
    Consumer -->|Upsert| DC & DP & DS & DSup & DD
    
    FC --> DC & DP & DS & DSup & DD
    
    DW -->|Query| Dashboard
    Dashboard --> Tab1 & Tab2

    style HashTable fill:#e1f5ff
    style Queue fill:#e1f5ff
    style Cache fill:#e1f5ff
    style FC fill:#ffe1e1
    style Dashboard fill:#e1ffe1
```

## Data Flow

```mermaid
sequenceDiagram
    participant Stream as CSV Stream
    participant Producer as Producer Thread
    participant Buffer as Hash Table
    participant Consumer as Hybrid Join
    participant Disk as Master Data
    participant DB as MySQL DB

    Stream->>Producer: Read transaction
    Producer->>Buffer: Hash by Product_ID
    
    loop Until Hash Full or Stream Empty
        Stream->>Producer: Read more
        Producer->>Buffer: Store in hash
    end
    
    Buffer->>Consumer: Trigger processing
    Consumer->>Disk: Load Partition
    
    loop For each product in partition
        Disk->>Consumer: Product details
        Consumer->>Buffer: Check hash[Product_ID]
        Buffer->>Consumer: All matching transactions
        Consumer->>DB: Enrich & Insert Facts
    end
    
    Consumer->>DB: Batch Commit (1000 records)
```

## Hybrid Join Algorithm

```mermaid
flowchart TD
    Start([Start]) --> Init[Initialize:<br/>- Hash Table<br/>- Queue<br/>- Partitions]
    Init --> ParallelStart{Launch Threads}
    
    ParallelStart -->|Thread 1| Producer[Producer:<br/>Read Stream]
    ParallelStart -->|Thread 2| Consumer[Consumer:<br/>Hybrid Join]
    
    Producer --> ReadCSV[Read CSV Line]
    ReadCSV --> Dedup{Duplicate?}
    Dedup -->|Yes| ReadCSV
    Dedup -->|No| PushBuffer[Push to Buffer]
    PushBuffer --> MoreData{More Data?}
    MoreData -->|Yes| ReadCSV
    MoreData -->|No| SetDone[Set Done Flag]
    
    Consumer --> CheckFull{Hash Full?}
    CheckFull -->|No| LoadHash[Load from Buffer]
    LoadHash --> HashInsert[Insert to Hash Table]
    HashInsert --> QueueAdd[Add to Queue]
    
    CheckFull -->|Yes| Probe[Probe Phase]
    QueueAdd --> Probe
    
    Probe --> GetOldest[Get Oldest from Queue]
    GetOldest --> FindPartition[Find Partition]
    FindPartition --> LoadPartition[Load Partition from Disk]
    LoadPartition --> JoinLoop{For each product}
    
    JoinLoop -->|Match| GetTxns[Get Buffered Transactions]
    GetTxns --> Enrich[Enrich with Master Data]
    Enrich --> CalcRevenue[Calculate Revenue]
    CalcRevenue --> InsertFact[Insert to FactSales]
    InsertFact --> RemoveHash[Remove from Hash]
    
    RemoveHash --> JoinLoop
    JoinLoop -->|Next| JoinLoop
    JoinLoop -->|Done| CheckBatch{Batch >= 1000?}
    
    CheckBatch -->|Yes| Commit[Database Commit]
    Commit --> CheckDone{Producer Done?}
    CheckBatch -->|No| CheckDone
    
    CheckDone -->|No| CheckFull
    CheckDone -->|Yes & Empty Hash| FinalCommit[Final Commit]
    FinalCommit --> End([End])

    style Init fill:#e1f5ff
    style LoadPartition fill:#ffe1e1
    style Commit fill:#e1ffe1
    style FinalCommit fill:#e1ffe1
```

## Component Details

### 1. ETL Layer
- **Producer Thread**: Reads CSV stream, deduplicates, pushes to buffer
- **Consumer Thread**: Implements Hybrid Join algorithm
- **Hash Table**: 10,000 slots for buffering transactions
- **FIFO Queue**: Ensures fair, first-in-first-out processing
- **Dimension Cache**: RAM-based lookup for foreign keys

### 2. Data Warehouse
- **Schema**: Star schema with 5 dimensions, 1 fact
- **Indexing**: All foreign keys indexed
- **Optimization**: Batch commits, cascading updates

### 3. Dashboard
- **Real-Time Tab**: Live metrics with auto-refresh
- **OLAP Tab**: 20 pre-built analytical queries
- **Visualization**: Plotly charts (line, bar, area)

## Performance Optimizations

| Optimization | Impact |
|--------------|--------|
| **Partitioning** | 99.6% reduction in disk reads |
| **RAM Caching** | 2.75M queries avoided |
| **Batch Commits** | 99.9% reduction in disk writes |
| **Hash Table** | O(1) lookup time |
| **FIFO Queue** | Fair processing, no starvation |

## Scalability

- **Horizontal**: Partition master data across multiple disks
- **Vertical**: Increase hash table size for larger streams
- **Parallel**: Multiple consumer threads per partition
- **Distributed**: Kafka for true streaming, Spark for processing
