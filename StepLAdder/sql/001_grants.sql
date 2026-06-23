/*
Run in the target Azure SQL Database.
Replace <FUNCTION_MANAGED_IDENTITY_NAME> with the exact managed identity display name.
If CREATE USER cannot resolve a duplicate display name, set @principal_object_id
to the managed identity principal/object id and keep @principal as a unique SQL user alias.
*/

DECLARE @principal SYSNAME = N'<FUNCTION_MANAGED_IDENTITY_NAME>';
DECLARE @principal_object_id NVARCHAR(36) = NULL; -- Example: N'00000000-0000-0000-0000-000000000000'

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_principals
    WHERE name = @principal
)
BEGIN
    DECLARE @create_user_sql NVARCHAR(4000);

    IF @principal_object_id IS NULL
    BEGIN
        SET @create_user_sql = N'CREATE USER [' + REPLACE(@principal, N']', N']]') + N'] FROM EXTERNAL PROVIDER;';
    END
    ELSE
    BEGIN
        SET @create_user_sql = N'CREATE USER [' + REPLACE(@principal, N']', N']]') + N'] WITH OBJECT_ID = ''' + REPLACE(@principal_object_id, N'''', N'''''') + N''';';
    END

    EXEC sp_executesql @create_user_sql;
END;

DECLARE @grant_view_state_sql NVARCHAR(4000) = N'GRANT VIEW DATABASE STATE TO [' + REPLACE(@principal, N']', N']]') + N'];';
DECLARE @grant_scoped_cfg_sql NVARCHAR(4000) = N'GRANT ALTER ANY DATABASE SCOPED CONFIGURATION TO [' + REPLACE(@principal, N']', N']]') + N'];';

EXEC sp_executesql @grant_view_state_sql;
EXEC sp_executesql @grant_scoped_cfg_sql;
