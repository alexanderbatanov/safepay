# SafePay
SafePay - fast and safe P2P payments
Insight Data Engineering project

[Presentation](https://docs.google.com/presentation/d/1_lU_j1KYGDEHNnhl2ZPPzT0R1si3TQVir_rS-OTf50U)

## Motivation

Main objective of my [Insight Data Engineering](http://insightdataengineering.com/) project was to explore how companies like PayPal process large volumes of transaction data in fast and safe manner. Not meeting these requirements could lead to negative consequences - either customers will flock or company's reputation is compromised. Meeting the requirements presents a technical challenge - time to evaluate compliance rules is bounded, yet all the rules need to be evaluated.

For SafePay project I used [Venmo](https://venmo.com/) data. The 62G dataset had 6 years worth of transactions. I ran experiments on a 100 day subset (1/1/17-4/18/17) with 44M transactions. The data itself was rather simple: (transaction_datetime, from_party, to_party).

The following business rules were codified:
* **Safe:** If a Party conducted more than a predefined number (30) of transactions in a given time interval (1 day), or if transaction volume grew significantly (3-fold) compared to previous time interval, the Party is blacklisted
* **Fast:** If either Party is blacklisted, transaction is denied

The second rule could be evaluated in real time, while the first rule requires batch data aggregation over a time window. Quick analysis revealed that most Parties conducted less than 3 transaction per day, with only a few were pushing out more than a 100.

## Technology Stack

**Spark** was a natural choice for processing engine. It supports streaming, data partitioning and parallel data aggregation (MapReduce). It is also possible to extend business rules with machine learning algorithms (MLlib) and payment graph processing (GraphX). **Kafka** with its ability to handle extremely high throughput was chosen as a messaging platform. **Cassandra** was selected for its ability to support fast writes into memory cache. 

![Data pipeline](diagrams/pipeline.png)

## Architecture

My other objective was to make solution architecture flexible by decoupling streaming and batch processing. Independent components are easier to evolve. SafePay jobs communicate via the database. The streaming job ("process_transactions.py") evaluates if Parties are blacklisted, marks up transaction records, and dumps them into a table. The batch job ("calculate_stats.py") at each iteration filters out transactions using one day window, compares volumes, and blacklist Parties based on the rules above.

![Data Model](diagrams/data_flow.png)

I had to re-factor my initial data model which had a single Party statistics table and two separate tables for approved and denied transactions. Sorted transactions order in Cassandra was removed to speed up database writes. Sorting, as well as separating approved and denied transactions, can be achieved by a batch job.

 Separating Sender and Receiver statistics calculations also made sense from a performance perspective. Aggregating Party data into a single table was the longest running job. Creating a single Party table could still be beneficial to expose a blacklisted Parties. Such "short" table can be brought in and persisted in Spark nodes memory as a dataframe.

## Results

Quick performance evaluation revealed that database access is the bottleneck. Current SafePay throughput is about 900 transactions/second. If database access is removed, it increases 3 times.

Since most parties do not conduct transactions at high frequency, Cassandra reads are likely to hit the disk. Blacklisted dataframe approach described above could solve this problem.

Using Spark dataframes to access Cassandra proved to be more performant then using prepared statements. With dataframes Spark is managing database connection pool. A trick was required to update Cassandra with immutable dataframes - temporary columns were added, calculated and renamed to match database schema.

Coordinating batch and streaming jobs presented an interesting challenge. Since Venmo data was replayed at about 200x speed (7 mins to push one day through), batch processing could potentially get ahead of streaming, and vice versa. If batch steps ahead faster, it will not have data to process in its' time window. One solution would be to create APIs that expose jobs processing state. Technologies such as [Airflow](https://airflow.apache.org/) could be used for job coordination as well.

In order to reduce possibility of non-compliant transactions getting through, time windows of streaming and batch jobs should be brought closer together. Streaming window can be extended with Spark Stateful Stream Processing (which may require additional memory on worker nodes). Batches can run more frequently by scaling out Spark.
