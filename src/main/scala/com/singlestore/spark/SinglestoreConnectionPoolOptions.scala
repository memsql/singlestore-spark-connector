package com.singlestore.spark

case class SinglestoreConnectionPoolOptions(enabled: Boolean,
                                            MaxOpenConns: Int,
                                            MaxIdleConns: Int,
                                            MinEvictableIdleTimeMs: Long,
                                            TimeBetweenEvictionRunsMS: Long,
                                            MaxWaitMS: Long,
                                            MaxConnLifetimeMS: Long)
