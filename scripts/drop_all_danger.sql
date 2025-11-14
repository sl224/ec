-- WARNING: This will delete all data in the 'etude_core' schema.
-- Use the correct database
USE [AnalyticsDataMart];
GO

-- 1. Drop all Foreign Key Constraints in the schema
DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql += N'ALTER TABLE ' 
    + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) 
    + N' DROP CONSTRAINT ' + QUOTENAME(fk.name) + N';' + CHAR(13)
FROM sys.foreign_keys AS fk
INNER JOIN sys.tables AS t ON fk.parent_object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE s.name = 'etude_core';

-- Print and execute the DROP CONSTRAINT statements
PRINT @sql;
EXEC sp_executesql @sql;
GO

-- 2. Drop all Tables in the schema
DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql += N'DROP TABLE ' 
    + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) 
    + N';' + CHAR(13)
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE s.name = 'etude_core';

-- Print and execute the DROP TABLE statements
PRINT @sql;
EXEC sp_executesql @sql;
GO

PRINT 'All tables in schema etude_core have been dropped.';