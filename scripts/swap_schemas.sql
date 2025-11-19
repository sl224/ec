-- SWAP SCRIPT: Promotes 'e2ude_core_dev' to 'e2ude_core'
-- 1. Archive current Production to Deprecated
-- 2. Promote Dev to Production

USE [AnalyticsDataMart];
GO

-- A. Ensure Archive Schema Exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'e2ude_core_deprecated')
BEGIN
    EXEC('CREATE SCHEMA [e2ude_core_deprecated]')
END
GO

-- B. Move existing Production tables to Deprecated
DECLARE @tableName NVARCHAR(256)
DECLARE @sql NVARCHAR(MAX)

DECLARE table_cursor CURSOR FOR
SELECT name
FROM sys.tables
WHERE schema_id = SCHEMA_ID('e2ude_core')

OPEN table_cursor
FETCH NEXT FROM table_cursor INTO @tableName

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = 'ALTER SCHEMA [e2ude_core_deprecated] TRANSFER [e2ude_core].[' + @tableName + ']'
    PRINT @sql
    EXEC sp_executesql @sql
    FETCH NEXT FROM table_cursor INTO @tableName
END

CLOSE table_cursor
DEALLOCATE table_cursor
GO

-- C. Move Dev tables to Production
DECLARE @tableNameDev NVARCHAR(256)
DECLARE @sqlDev NVARCHAR(MAX)

DECLARE dev_cursor CURSOR FOR
SELECT name
FROM sys.tables
WHERE schema_id = SCHEMA_ID('e2ude_core_dev')

OPEN dev_cursor
FETCH NEXT FROM dev_cursor INTO @tableNameDev

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sqlDev = 'ALTER SCHEMA [e2ude_core] TRANSFER [e2ude_core_dev].[' + @tableNameDev + ']'
    PRINT @sqlDev
    EXEC sp_executesql @sqlDev
    FETCH NEXT FROM dev_cursor INTO @tableNameDev
END

CLOSE dev_cursor
DEALLOCATE dev_cursor
GO

PRINT 'Swap Complete. Old Production is now in [e2ude_core_deprecated]. Dev is now [e2ude_core].'
