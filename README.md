# SafePay
SafePay - fast and safe P2P payments
Insight Data Engineering project

[Presentation](https://docs.google.com/presentation/d/1_lU_j1KYGDEHNnhl2ZPPzT0R1si3TQVir_rS-OTf50U)

## Motivation

An objectives of my [Insight Data Engineering](http://insightdataengineering.com/) project was to explore how companies such as PayPal process large volumes transaction data in fast and safe manner. Not meeting **Fast** and **Safe** requirements may create negative impact - either customers will flock or company's reputation is compromised. Meeting both requirements represents a technical challenge. Time to evaluate compliance rules is bounded, yet all the rules need to be evaluated.

For SafePay project I used [Venmo](https://venmo.com/) data. The dataset (62G) covered 6 year time period. I ran experiments on a subset of about 100 days (1/1/17-4/18/17) with 44M transactions. The data itself was rather simple: (datetime, from_party, to_party).

SafePay project codified simple business rules:
* **Safe:** If a Party conducted more than a predefined number (e.g. 30) of transactions in a given time interval (e.g. 1 day), or if transaction volume grew significantly (e.g. 3-fold) compared to previous time interval, the party is blacklisted
* **Fast:** If either Party is blacklisted, transaction is denied (and sent for further investigation)

The second rule could be evaluated in real time, while the first rule requires batch data aggregation over time window. Quick analysis of dataset revealed that most Parties conducted less 3 transaction per day, with only a few senders pushing out more than a 100 payments. Such transactions would be denied by SafPay (until their validity is established, e.g. small business payroll).

## Stack

The business problem called for both real-time and batch operations, and **Spark** was a natural choice as a processing engine that supports streaming, data partitioning and parallelized data aggregation. Spark would also allow to extend business rules with machine learning algorithms via MLlib and payment graph computation via GraphX. Assuming that processing transactions from multiple channels (e.g. mobile, web, POS) is centralized and no transaction routing is required, **Kafka** with its ability to handle extremely high throughput was chosen a messaging platform. **Cassandra** database was selected for its ability to support fast writes into memory cache. 

![Data pipeline](diagrams/pipeline.png)

## Architecture

One of my objectives was to make architecture flexible by decoupling streaming and batch jobs, so that they can evolve independently. In the current implementation the jobs are not aware of each other and communicate via the database only. The streaming job ("process_transactions.py") evaluates if Parties are blacklisted, marks up transaction records, and dumps them into a table. At each iteration the batch job ("calculate_stat.py") extracts one day worth of transactions (using UTC datetime stamps), compares new and old transaction volumes, and decides if Parties have to be blacklisted according to the rules specified above.  

![Data Model](diagrams/data_flow.png)

My initial data model had a single Party statistics table and two separate tables for approved and denied transactions (the latter would be reviewed by compliance team). Transaction tables were sorted on datetime stamps. This approach was changed to the current model to speed up database writes. Approved and denied transactions records can be split by and sorted by a batch job. Calculating senders and receivers statistics independently can be parallelized. Note that it is still may be worth while building a single "short" table of all blacklisted parties and bringing it into Spark Streaming memory for each micro-batch processed. This type of Party data aggregation was running the longest, since it requires finding uniques in a joined list of Senders and Receivers.

## Results

Quick performance evaluation revealed that database access appears to be the bottleneck. Since most parties do not conduct transactions at high frequency, Cassandra reads would most likely result in disk access as opposed to in-memory cache. Current throughput is less then 1000 transactions/second. It increases 3 times if database access is removed from the equation.

It is worth noting that using Spark dataframes for database access (both reads and writes) proved to be more performant compared to using prepared statements. This is probably due to the fact that Spark is managing database connection pool. Using immutable dataframes for updates and inserts required a trick. Temporary columns were added to dataframes to store calculation results, and then renamed according to database schema (original columns had to be dropped).  

Venmo data was replayed at about x200 speed (7 min for one day's worth of transactions). This made it difficlt to schedule the batch job using crontab. If it would get ahead of the streaming job, its time window would only be partially filled with transactions, or even empty. If it would lag behind, not all non-compliant Parties would be blacklisted in time. In production environment coordination between streaming and batch jobs would be required. Both can be wrapped in APIs that expose their processing state. In general, to reduce the possibility of non-compliant transactions getting through, time windows of streaming and batch jobs should be brought closer. On the streaming side this can be achieved with Spark Stateful Stream Processing which will require more memory on worker nodes. On the batch side running more batches in parallel more frequently (i.e. scaling out) could provide the solution.
