**Database Health Reference Procedure**

The proc_DB_Health_reference procedure is a comprehensive SQL Server stored procedure that provides useful information related to the health and performance of a database. It includes multiple sections that cover different aspects of the database's health, such as CPU utilization, execution statistics, memory consumption, statistics update status, partition table status, table space usage, index fragmentation, and long-running queries.

**Sections Included**

**Run Time CPU Utilisation:** This section calculates the CPU utilization of each database and presents it as a percentage.
**Execution Statistics:** It provides information about the average CPU usage and average logical reads of executed queries, along with the number of execution plans and total executions.
**Database Memory Consumption:** This section displays the databases and their corresponding memory consumption in the buffer pool.
**Statistics Last Updated:** It shows when the statistics for each table were last executed.
**Partition Table Status:** It provides information about the partition status of tables, including the maximum partition number, minimum unused partition number, and partition disturbance.
**Table Space Usage:** This section presents the space used by each table in the database.
**Index Fragmentation and Rebuilding:** It lists indexes with fragmentation percentages higher than 50% and suggests rebuilding or reorganizing them.
**Long Running Queries:** This section identifies long-running queries, displaying the query text, query plan, and related execution statistics.

**Usage**
The proc_DB_Health_reference procedure can be executed in SQL Server Management Studio or any SQL Server client tool. Simply run the procedure, and it will provide the relevant health information for the database.

Note: Make sure you have the necessary permissions to execute the procedure and access the system views and dynamic management views used within it.

Feel free to customize or modify the procedure according to your specific needs.

Please note that the provided code is a reference and should be thoroughly tested and adapted to your environment before using it in a production setting.

**License**
This code is provided under the MIT License. You are free to use, modify, and distribute it as per the terms of the license.
