create procedure proc_DB_Health_reference
as
--//==================================
--Run Time CPU Utilisation
--//==================================

WITH CPU_Per_Db
AS
(SELECT
dmpa.DatabaseID
, DB_Name(dmpa.DatabaseID) AS [Database]
, SUM(dmqs.total_worker_time) AS CPUTimeAsMS
FROM sys.dm_exec_query_stats dmqs
CROSS APPLY
(SELECT
CONVERT(INT, value) AS [DatabaseID]
FROM sys.dm_exec_plan_attributes(dmqs.plan_handle)
WHERE attribute = N'dbid') dmpa
GROUP BY dmpa.DatabaseID)
 
SELECT
[Database]
,[CPUTimeAsMS]
,CAST([CPUTimeAsMS] * 1.0 / SUM([CPUTimeAsMS]) OVER() * 100.0 AS DECIMAL(5, 2)) AS [CPUTimeAs%]
FROM CPU_Per_Db
ORDER BY [CPUTimeAsMS] DESC;


;WITH cte AS
(
SELECT stat.[sql_handle],
stat.statement_start_offset,
stat.statement_end_offset,
COUNT(*) AS [NumExecutionPlans],
SUM(stat.execution_count) AS [TotalExecutions],
((SUM(stat.total_logical_reads) * 1.0) / SUM(stat.execution_count)) AS [AvgLogicalReads],
((SUM(stat.total_worker_time) * 1.0) / SUM(stat.execution_count)) AS [AvgCPU]
FROM sys.dm_exec_query_stats stat
GROUP BY stat.[sql_handle], stat.statement_start_offset, stat.statement_end_offset
)
SELECT CONVERT(DECIMAL(15, 5), cte.AvgCPU) AS [AvgCPU],
CONVERT(DECIMAL(15, 5), cte.AvgLogicalReads) AS [AvgLogicalReads],
cte.NumExecutionPlans,
cte.TotalExecutions,
DB_NAME(txt.[dbid]) AS [DatabaseName],
OBJECT_NAME(txt.objectid, txt.[dbid]) AS [ObjectName],
SUBSTRING(txt.[text], (cte.statement_start_offset / 2) + 1,
(
(CASE cte.statement_end_offset
WHEN -1 THEN DATALENGTH(txt.[text])
ELSE cte.statement_end_offset
END - cte.statement_start_offset) / 2
) + 1
)
FROM cte
CROSS APPLY sys.dm_exec_sql_text(cte.[sql_handle]) txt
ORDER BY cte.AvgCPU DESC;

--//--------------------------------------------------
--database that consumes highest memory in buffer pool
--//--------------------------------------------------

SELECT
COUNT(*) AS cached_pages_count ,
( COUNT(*) * 8.0 ) / 1024 AS MB ,
CASE database_id
 WHEN 32767 THEN 'ResourceDb'
 ELSE DB_NAME(database_id)
END AS Database_name
FROM sys.dm_os_buffer_descriptors
GROUP BY database_id
order by MB desc

--//--------------------------------------------------
--How to check when statistics was last executed?
--//--------------------------------------------------

SELECT t.name TableName, s.[name] StatName, STATS_DATE(t.object_id,s.[stats_id]) LastUpdated
FROM sys.[stats] AS s
JOIN sys.[tables] AS t
ON [s].[object_id] = [t].[object_id]
WHERE t.type = 'u'

--//--------------------
--Partition table status
--//--------------------

select
t.name as Table_Name,
max(p.partition_number) as Max_Partition_Number,
min(case when rows=0 then partition_number else null end) as Min_Unused_Partition_Number,
case
when min(case when rows=0 then partition_number else null end) is null then 'Critical'
when max(p.partition_number)-min(case when rows=0 then partition_number else null end)<=2 then 'Alert'
else '-'
end as Partition_Status,
sum(case
when charindex(' ',cast(pr.value as varchar)) > 0 then 0
when charindex(' ',cast(pr.value as varchar)) = 0 then (case when pr.value<>p.rows and p.rows<>0 and p.index_id=0 then 1 else 0 end)
else 0
end) as PartionDisturbed,
pr.value as LowerBoundaryValue,
ds.name as PartitionScheme,
pf.name as PartitionFunction
from
sys.partitions p
join sys.tables t on p.object_id = t.object_id
JOIN sys.indexes AS i ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id
JOIN sys.partition_schemes AS ps ON ps.data_space_id = ds.data_space_id
JOIN sys.partition_functions AS pf ON pf.function_id = ps.function_id
left join sys.partition_range_values pr  on ps.function_id=pr.function_id and pr.boundary_id=1
where
OBJECTPROPERTY(p.object_id, 'ISMSShipped') = 0
group by
t.name,ds.name,pf.name,pr.value

--//==================================
--Table Spce used
--//==================================

-- Create the temporary table...
CREATE TABLE #tblResults
(
  [name]   nvarchar(150),
  [rows]   int,
  [reserved]   varchar(18),
  [reserved_int]   int default(0),
  [data]   varchar(18),
  [data_int]   int default(0),
  [index_size]   varchar(18),
  [index_size_int]   int default(0),
  [unused]   varchar(18),
  [unused_int]   int default(0)
)


-- Populate the temp table...
EXEC sp_MSforeachtable @command1=
"INSERT INTO #tblResults
  ([name],[rows],[reserved],[data],[index_size],[unused])
 EXEC sp_spaceused '?'"
   
-- Strip out the " KB" portion from the fields
UPDATE #tblResults SET
  [reserved_int] = CAST(SUBSTRING([reserved], 1,
CHARINDEX(' ', [reserved])) AS int),
  [data_int] = CAST(SUBSTRING([data], 1,
CHARINDEX(' ', [data])) AS int),
  [index_size_int] = CAST(SUBSTRING([index_size], 1,
CHARINDEX(' ', [index_size])) AS int),
  [unused_int] = CAST(SUBSTRING([unused], 1,
CHARINDEX(' ', [unused])) AS int)
   
-- Return the results...
SELECT * FROM #tblResults order by reserved_int desc
drop Table #tblResults


--//========================================
-- DB index fragmentation and rebuilding
--//========================================

SELECT
dbschemas.[name] as 'Schema',
dbtables.[name] as 'Table',
dbindexes.[name] as 'Index',
indexstats.avg_fragmentation_in_percent,
indexstats.page_count
FROM
sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, NULL) AS indexstats
INNER JOIN sys.tables dbtables on dbtables.[object_id] = indexstats.[object_id]
INNER JOIN sys.schemas dbschemas on dbtables.[schema_id] = dbschemas.[schema_id]
INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id]
AND indexstats.index_id = dbindexes.index_id
WHERE
indexstats.database_id = DB_ID()
--AND dbtables.[name] like '%studentlogin%'
and dbindexes.[name] is not null
and indexstats.avg_fragmentation_in_percent>=50
ORDER BY
[Table],indexstats.avg_fragmentation_in_percent desc

-- use this only to rebuild indexes : ALTER INDEX ALL ON CDCFrequencyParameterValues REBUILD

-- Not use this if not needed : ALTER INDEX ALL ON CDCFrequencyParameterValues REORGANIZE

--//------------------------
-- For Long running queries
--//------------------------

--select spid, db_name(dbid), * from master..sysprocesses

--select spid, db_name(dbid) as dbname, program_name, loginame, * from master..sysprocesses where dbid = 42 --this would be your database id

SELECT  st.text,
qp.query_plan,
qs.*
FROM    
(
SELECT  TOP 50 *
FROM    sys.dm_exec_query_stats
ORDER BY total_worker_time DESC
) AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE qs.max_worker_time > 300 OR qs.max_elapsed_time > 300
