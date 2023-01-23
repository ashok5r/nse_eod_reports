# Requirement
--------------------------------------
- **Provider :** Harish R
- **Developer :** Ashok R
- **Language :** Python
- **Framework :** PySpark

> Read NSE securities behave data and indices in pyspark, save combining both to output location

> File 1 : sec_bhavdata_full_09012023.csv
> File 2 : ind_close_all_09012023.csv

> Input Location : ./in/20230109
> Output Location : ./out/20230109

-----------------------------
### Create a python script ***nse_sec_eod_reports.py*** & Follow below steps to generate results
-----------------------------
1. Read path ./in/20230109/sec_bhavdata_full_09012023.csv with options header=true and inferSchema=true as csv into dataframe variable sec_behave_data
2. The schema should look like below, sec_behave_data.printSchema()
    ```
        root
        |-- SYMBOL: string (nullable = true)
        |--  SERIES: string (nullable = true)
        |--  DATE1: string (nullable = true)
        |--  PREV_CLOSE: double (nullable = true)
        |--  OPEN_PRICE: string (nullable = true)
        |--  HIGH_PRICE: string (nullable = true)
        |--  LOW_PRICE: string (nullable = true)
        |--  LAST_PRICE: double (nullable = true)
        |--  CLOSE_PRICE: double (nullable = true)
        |--  AVG_PRICE: double (nullable = true)
        |--  TTL_TRD_QNTY: string (nullable = true)
        |--  TURNOVER_LACS: string (nullable = true)
        |--  NO_OF_TRADES: integer (nullable = true)
        |--  DELIV_QTY: string (nullable = true)
        |--  DELIV_PER: string (nullable = true)
    ```
 3. Read ./in/20230109/indices/ind_close_all_09012023.csv with options header=true and inferSchema=true as csv into dataframe variable nse_indicies
 4. The schema should look like below, nse_indices.printSchema()
    ```
        root
        |-- Index Name: string (nullable = true)
        |-- Index Date: string (nullable = true)
        |-- Open Index Value: string (nullable = true)
        |-- High Index Value: string (nullable = true)
        |-- Low Index Value: string (nullable = true)
        |-- Closing Index Value: double (nullable = true)
        |-- Points Change: double (nullable = true)
        |-- Change(%): string (nullable = true)
        |-- Volume: string (nullable = true)
        |-- Turnover (Rs. Cr.): string (nullable = true)
        |-- P/E: string (nullable = true)
        |-- P/B: string (nullable = true)
        |-- Div Yield: string (nullable = true)
    ```
 5. In file nse_indicies, rename column name "Index Name" to "SYMBOL"
 6. In file nse_indicies, add new column "SERIES" with default value "INDEX"
 7. In file nse_indicies, add new column "DATE1" with value "Index Date" in the format 'dd-MM-yyyy' to 'dd-MMM-yyyy', example '09-01-2023' value to '09-Jan-2023'
 8. In file nse_indicies, rename column name "Open Index Value" to "OPEN_PRICE"
 9. In file nse_indicies, rename column name "High Index Value" to "HIGH_PRICE"
 10. In file nse_indicies, rename column name "Low Index Value" to "LOW_PRICE"
 11. In file nse_indicies, rename column name "Closing Index Value" to "CLOSE_PRICE"
 12. In file nse_indicies, add new column "LAST_PRICE" with value "Closing Index Value"
 13. In file nse_indicies, add new column "AVG_PRICE" with value "Closing Index Value"
 14. In file nse_indicies, rename column name "Volume" to "TTL_TRD_QNTY"
 15. In file nse_indicies, add new column "TURNOVER_LACS" with value "Turnover (Rs. Cr.)" multiply by 10
 16. In file nse_indicies, add new column "NO_OF_TRADES" with default value 0
 17. In file nse_indicies, add new column "DELIV_QTY" with default value 0
 18. In file nse_indicies, add new column "DELIV_PER" with default value 0
 19. In file nse_indicies, add new column "PREV_CLOSE" with value "Closing Index Value" - "Points Change" (substract 'Points Change' from 'Closing Index Value')
 20. Into new dataframe variable nse_indices_new, from nse_indicies -> select SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER
 21. Union By Name dataframe sec_behave_data and nse_indices_new as variable sec_behave_data_all, using -> sec_behave_data.unionByName(nse_indices_new)
 22. print distinct SERIES from sec_behave_data_all, ie -> sec_behave_data_all.distinct.select("SERIES").show(false)
        ``` 
        +-------+
        | SERIES|
        +-------+
        | EQ    |
        | INDEX |
        | ...   |
        | ...   |
        +-------+
        ```
23. save sec_behave_data_all to the location "./out/20230109" as csv with repartition to 1, sec_behave_data_all.repartition(1).format("csv").save("./out/20230109")

-------------------------------------------------------------------------