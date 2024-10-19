## Machine

- CPU: AMD Ryzen 5 5600G with Radeon Graphics
- Memory: 32GB DDR4 3600 MT/s
- Postgres Database: docker.io/postgis/postgis

## Results
Difference is `(KDBC - JDBC)/JDBC * 100`

| Benchmark                                       | Mode | Cnt | Score      | Error      | Units | % difference vs JDBC           |
|-------------------------------------------------|------|-----|------------|------------|-------|--------------------------------|
| PgBenchmarkAsyncCopyKdbc.copyInSource           | avgt | 40  | 105289.646 | ± 2980.874 | us/op | -0.4                           |
| PgBenchmarkAsyncCopyKdbc.copyInStream           | avgt | 40  | 105062.501 | ± 2930.942 | us/op | -0.6                           |
| PgBenchmarkAsyncCopyKdbc.copyOutSink            | avgt | 40  | 21343.902  | ±  587.863 | us/op | 5.32                           |
| PgBenchmarkAsyncCopyKdbc.copyOutStream          | avgt | 40  | 22246.791  | ±  587.863 | us/op | 9.78                           |
| PgBenchmarkAsyncMultiJdbc.querySingleRow        | avgt | 40  | 1581.225   | ±   16.340 | us/op | N/A                            |
| PgBenchmarkAsyncMultiKdbc.querySingleRow        | avgt | 40  | 1791.724   | ±   12.911 | us/op | 13.31 (async) 23.86 (blocking) |
| PgBenchmarkAsyncSingleJdbc.queryMultipleRows    | avgt | 40  | 83.682     | ±    1.012 | us/op | N/A                            |
| PgBenchmarkAsyncSingleJdbc.querySingleRow       | avgt | 40  | 59.152     | ±    1.238 | us/op | N/A                            |
| PgBenchmarkAsyncSingleKdbc.queryMultipleRows    | avgt | 40  | 92.264     | ±    0.552 | us/op | 10.26 (async) 31.02 (blocking) |
| PgBenchmarkAsyncSingleKdbc.querySingleRow       | avgt | 40  | 69.633     | ±    1.257 | us/op | 17.72 (async) 45.77 (blocking) |
| PgBenchmarkBlockingCopyJdbc.copyIn              | avgt | 40  | 105741.557 | ± 2944.709 | us/op | N/A                            |
| PgBenchmarkBlockingCopyJdbc.copyOut             | avgt | 40  | 20264.972  | ±  523.915 | us/op | N/A                            |
| PgBenchmarkBlockingMultiJdbc.querySingleRow     | avgt | 40  | 1446.570   | ±    6.007 | us/op | N/A                            |
| PgBenchmarkBlockingSingleJdbc.queryMultipleRows | avgt | 40  | 70.419     | ±    0.558 | us/op | N/A                            |
| PgBenchmarkBlockingSingleJdbc.querySingleRow    | avgt | 40  | 47.769     | ±    1.248 | us/op | N/A                            |