## Cluster setup

* 7 Machines on AWS - identical spec
    * m4.4xlarge
    * 1 ebs data volume @ 256 GB
    * ext4
    * provisioned iops 2000
* Launched via cloud.memsql.com
* Shared vpc
* Launched with placement group

## Testing Method

* Create the database and tables using the provided schema.sql
* Load the data using MemSQL Loader and the provided spec files
  (rankings.json, uservisits.json)
* Deploy the MemSQL spark distribution via MemSQL-Ops
* Stop the MemSQL Spark Interface
* Run spark-shell using MemSQL Ops (preloading the prelude file)
    * `memsql-ops spark-shell -- -i prelude.scala`
* Pre-cache the tables in spark
    * `Prelude.cacheTables`
* Run each query once to warm it up
    * `Queries.qX` (replace X with the query number)
* Run each query three times to get the numbers below
    * `Queries.qX` (replace X with the query number)

## Results

### Q1

```sql
select count(*) from(
    select pageURL, pageRank from rankings where pageRank > 1000
) x
```

Query   | Pushdown? | Time (ms)
--------|-----------|----------
1       | no        | 403.50
1       | no        | 384.16
1       | no        | 322.19
1       | yes       | 86.94
1       | yes       | 88.66
1       | yes       | 74.27

### Q2

```sql
select searchWord, sum(adRevenue) as totalRevenue
from uservisits uv, rankings r
where uv.destURL = r.pageURL
  and searchWord like "north%"
group by searchWord
order by totalRevenue desc
limit 10
```

Query   | Pushdown? | Time (ms)
--------|-----------|----------
2       | no        | 6005.78
2       | no        | 9216.92
2       | no        | 6481.49
2       | yes       | 155.25
2       | yes       | 158.55
2       | yes       | 132.27

### Q3

```sql
select destURL, count(*) as numVisitors, sum(adRevenue) as adTotal, sum(duration) as spentOnPage
from uservisits uv
where uv.destURL LIKE "uh%"
group by destURL
order by adTotal desc
limit 10
```

Query   | Pushdown? | Time (ms)
--------|-----------|----------
3       | no        | 846.35
3       | no        | 2232.42
3       | no        | 2024.86
3       | yes       | 156.88
3       | yes       | 159.28
3       | yes       | 97.43

### Q4

```sql
select
    destURL, count(*) as numVisitors, sum(adRevenue) as adTotal, sum(duration) as spentOnPage
from
    uservisits uv
    inner join rankings r
on uv.destURL = r.pageURL
where
    uv.destURL LIKE "uh%"
group by destURL
order by adTotal desc
limit 10
```

Query   | Pushdown? | Time (ms)
--------|-----------|----------
4       | no        | 5230.33
4       | no        | 5971.84
4       | no        | 6508.28
4       | yes       | 179.29
4       | yes       | 313.33
4       | yes       | 135.62
