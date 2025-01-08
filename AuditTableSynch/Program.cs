using System;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Binder;
using Microsoft.Extensions.Logging;
using System.Data.Common;

namespace AuditTableSynch;

public class ColumnDifference
{
    public string ColumnName { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public short MaxLength { get; set; }
    public byte Precision { get; set; }
    public byte Scale { get; set; }
    public bool IsNullable { get; set; }
}

public class DatabaseAuditSync
{
    private readonly string _connectionString;
    private readonly ILogger<DatabaseAuditSync> _logger;
    private readonly IConfiguration _configuration;
    private readonly List<string> _tablesToUpdate;

    public DatabaseAuditSync(IConfiguration configuration, ILogger<DatabaseAuditSync> logger)
    {
        _configuration = configuration;
        _connectionString = configuration.GetConnectionString("DefaultConnection") 
            ?? throw new ArgumentNullException(nameof(configuration));
        _logger = logger;
        _tablesToUpdate = configuration.GetSection("AuditSync:TablesToUpdate").Get<List<string>>() ?? new List<string>();
    }

    public class TableAuditReport
    {
        public string TableName { get; set; } = string.Empty;
        public string AuditTableName { get; set; } = string.Empty;
        public List<ColumnDifference> Differences { get; set; } = new();
        public bool IsSelectedForUpdate { get; set; }
    }

    public async Task<List<TableAuditReport>> AnalyzeTablesAsync()
    {
        var reports = new List<TableAuditReport>();
        var tables = await GetTablesWithAuditTablesAsync();

        foreach (var (tableName, auditTableName) in tables)
        {
            var differences = await GetColumnDifferencesAsync(tableName, auditTableName);
            var isSelectedForUpdate = _tablesToUpdate.Contains(tableName, StringComparer.OrdinalIgnoreCase);

            var report = new TableAuditReport
            {
                TableName = tableName,
                AuditTableName = auditTableName,
                Differences = differences,
                IsSelectedForUpdate = isSelectedForUpdate
            };

            if (differences.Any())
            {
                LogTableDifferences(report);
            }

            reports.Add(report);
        }

        return reports;
    }

    private void LogTableDifferences(TableAuditReport report)
    {
        _logger.LogInformation("Found differences in table: {TableName}", report.TableName);
        
        foreach (var diff in report.Differences)
        {
            _logger.LogInformation("Column: {ColumnName}, Type: {DataType}, MaxLength: {MaxLength}, " +
                "Precision: {Precision}, Scale: {Scale}, IsNullable: {IsNullable}",
                diff.ColumnName, diff.DataType, diff.MaxLength, diff.Precision, diff.Scale, diff.IsNullable);
        }

        _logger.LogInformation("Table is {Status} for updates", 
            report.IsSelectedForUpdate ? "selected" : "not selected");
    }

    public async Task ProcessSelectedTablesAsync()
    {
        var reports = await AnalyzeTablesAsync();
        var selectedReports = reports.Where(r => r.IsSelectedForUpdate && r.Differences.Any());

        foreach (var report in selectedReports)
        {
            try
            {
                _logger.LogInformation("Processing table {TableName}", report.TableName);
                
                await AddMissingColumnsAsync(report.AuditTableName, report.Differences);
                await UpdateTriggersAsync(report.TableName);
                
                _logger.LogInformation("Successfully updated table {TableName}", report.TableName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing table {TableName}", report.TableName);
                throw;
            }
        }
    }

    public async Task<List<(string TableName, string AuditTableName)>> GetTablesWithAuditTablesAsync()
    {
        var query = @"
            SELECT 
                t1.name AS TableName,
                t2.name AS AuditTableName
            FROM sys.tables t1
            INNER JOIN sys.tables t2 
                ON t2.name = t1.name + '_Audit'
            WHERE t2.name LIKE '%_Audit'
            ORDER BY t1.name;";

        var tables = new List<(string TableName, string AuditTableName)>();
        
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        using var command = new SqlCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            tables.Add((
                reader.GetString(0),
                reader.GetString(1)
            ));
        }
        
        return tables;
    }

    public async Task<List<ColumnDifference>> GetColumnDifferencesAsync(string tableName, string auditTableName)
    {
        var query = @"
            SELECT 
                c1.name AS ColumnName,
                t.name AS DataType,
                c1.max_length AS MaxLength,
                c1.precision AS Precision,
                c1.scale AS Scale,
                c1.is_nullable AS IsNullable
            FROM sys.columns c1
            INNER JOIN sys.types t ON c1.user_type_id = t.user_type_id
            LEFT JOIN sys.columns c2 
                ON c2.name = c1.name 
                AND c2.object_id = OBJECT_ID(@AuditTableName)
            WHERE c1.object_id = OBJECT_ID(@TableName)
            AND c2.column_id IS NULL
            AND c1.name NOT IN ('AuditAction', 'AuditDate', 'AuditUser', 'AuditApp');";

        var differences = new List<ColumnDifference>();
        
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        using var command = new SqlCommand(query, connection);
        command.Parameters.AddWithValue("@TableName", tableName);
        command.Parameters.AddWithValue("@AuditTableName", auditTableName);
        
        using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            differences.Add(new ColumnDifference
            {
                ColumnName = reader.GetString(0),
                DataType = reader.GetString(1),
                MaxLength = reader.GetInt16(2),
                Precision = reader.GetByte(3),
                Scale = reader.GetByte(4),
                IsNullable = reader.GetBoolean(5)
            });
        }
        
        return differences;
    }

    public async Task AddMissingColumnsAsync(string auditTableName, List<ColumnDifference> differences)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        foreach (var diff in differences)
        {
            var nullableStr = diff.IsNullable ? "NULL" : "NOT NULL";
            var lengthStr = GetLengthString(diff);

            var query = $@"
                ALTER TABLE {auditTableName} 
                ADD {diff.ColumnName} {diff.DataType}{lengthStr} {nullableStr};";

            try
            {
                using var command = new SqlCommand(query, connection);
                await command.ExecuteNonQueryAsync();
                _logger.LogInformation("Added column {ColumnName} to {TableName}", 
                    diff.ColumnName, auditTableName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error adding column {ColumnName} to {TableName}", 
                    diff.ColumnName, auditTableName);
                throw;
            }
        }
    }

    private string GetLengthString(ColumnDifference diff)
    {
        return diff.DataType.ToLower() switch
        {
            "nvarchar" or "varchar" or "char" or "nchar" 
                => diff.MaxLength == -1 ? "(MAX)" : $"({diff.MaxLength})",
            "decimal" or "numeric" 
                => $"({diff.Precision},{diff.Scale})",
            _ => string.Empty
        };
    }

    public async Task UpdateTriggersAsync(string tableName)
    {
        var auditTableName = $"{tableName}_Audit";
        var columns = await GetTableColumnsAsync(tableName);
        
        await UpdateInsertTriggerAsync(tableName, auditTableName, columns);
        await UpdateModifyTriggerAsync(tableName, auditTableName, columns);
        await UpdateDeleteTriggerAsync(tableName, auditTableName, columns);
    }

    private async Task<List<string>> GetTableColumnsAsync(string tableName)
    {
        var query = @"
            SELECT c.name
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(@TableName)
            AND c.name NOT IN ('AuditAction', 'AuditDate', 'AuditUser', 'AuditApp')
            ORDER BY c.column_id;";

        var columns = new List<string>();
        
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        using var command = new SqlCommand(query, connection);
        command.Parameters.AddWithValue("@TableName", tableName);
        
        using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            columns.Add(reader.GetString(0));
        }
        
        return columns;
    }

    private async Task UpdateInsertTriggerAsync(string tableName, string auditTableName, List<string> columns)
    {
        var columnList = string.Join(", ", columns);
        var trigger = $@"
            CREATE OR ALTER TRIGGER [tr_{tableName}_Insert]
            ON [{tableName}]
            AFTER INSERT
            AS
            BEGIN
                SET NOCOUNT ON;
                
                INSERT INTO [{auditTableName}] (
                    {columnList},
                    AuditAction
                )
                SELECT 
                    {columnList},
                    'I'
                FROM INSERTED;
            END";

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var command = new SqlCommand(trigger, connection);
            await command.ExecuteNonQueryAsync();
            _logger.LogInformation("Updated INSERT trigger for table {TableName}", tableName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating INSERT trigger for table {TableName}", tableName);
            throw;
        }
    }

    private async Task UpdateModifyTriggerAsync(string tableName, string auditTableName, List<string> columns)
    {
        var columnList = string.Join(", ", columns);
        var trigger = $@"
            CREATE OR ALTER TRIGGER [tr_{tableName}_Modify]
            ON [{tableName}]
            AFTER UPDATE
            AS
            BEGIN
                SET NOCOUNT ON;
                
                INSERT INTO [{auditTableName}] (
                    {columnList},
                    AuditAction
                )
                SELECT 
                    {columnList},
                    'U'
                FROM INSERTED;
            END";

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var command = new SqlCommand(trigger, connection);
            await command.ExecuteNonQueryAsync();
            _logger.LogInformation("Updated MODIFY trigger for table {TableName}", tableName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating MODIFY trigger for table {TableName}", tableName);
            throw;
        }
    }

    private async Task UpdateDeleteTriggerAsync(string tableName, string auditTableName, List<string> columns)
    {
        var columnList = string.Join(", ", columns);
        var trigger = $@"
            CREATE OR ALTER TRIGGER [tr_{tableName}_Delete]
            ON [{tableName}]
            AFTER DELETE
            AS
            BEGIN
                SET NOCOUNT ON;
                
                INSERT INTO [{auditTableName}] (
                    {columnList},
                    AuditAction
                )
                SELECT 
                    {columnList},
                    'D'
                FROM DELETED;
            END";

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var command = new SqlCommand(trigger, connection);
            await command.ExecuteNonQueryAsync();
            _logger.LogInformation("Updated DELETE trigger for table {TableName}", tableName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating DELETE trigger for table {TableName}", tableName);
            throw;
        }
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        // Set up configuration
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json")
            .Build();

        // Set up logging
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddConfiguration(configuration.GetSection("Logging"))
                .AddConsole()
                .AddDebug();
        });

        var logger = loggerFactory.CreateLogger<DatabaseAuditSync>();
        var dbAuditSync = new DatabaseAuditSync(configuration, logger);

        try
        {
            // First, analyze all tables and log differences
            var reports = await dbAuditSync.AnalyzeTablesAsync();
            
            // Then, process only the selected tables
            await dbAuditSync.ProcessSelectedTablesAsync();

            // Output summary
            var tablesWithDifferences = reports.Count(r => r.Differences.Any());
            var tablesUpdated = reports.Count(r => r.IsSelectedForUpdate && r.Differences.Any());
            
            logger.LogInformation(
                "Analysis complete. Found {DifferenceCount} tables with differences. Updated {UpdatedCount} tables.", 
                tablesWithDifferences, 
                tablesUpdated);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred during the audit sync process");
            throw;
        }
    }
}