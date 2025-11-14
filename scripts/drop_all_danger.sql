-- Danger: This script irreversibly removes all objects in the 'etude_core' schema.
-- Confirm you're connected to the intended database and have reliable backups.
-- Intended for controlled maintenance; obtain approvals before executing.
USE [AnalyticsDataMart];
GO

-- 1) Drop all foreign-key constraints in the target schema.
-- Collect ALTER TABLE ... DROP CONSTRAINT statements into @sql and execute as a batch.
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

-- 2) Drop all tables in the target schema. Run after constraints are removed.
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

PRINT 'All tables in schema etude_core have been dropped. Confirm expected outcome and restore from backup if necessary.';