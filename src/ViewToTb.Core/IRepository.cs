
namespace ViewToTb.Core
{
    internal interface IRepository
    {
        Task<ICollection<SourceView>> GetViewsAsync(string viewSchema, CancellationToken stoppingToken);
        Task<ICollection<SourceTable>> GetSourceTablesAsync(SourceView view, CancellationToken stoppingToken);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="view"></param>
        /// <param name="targetSchema">to store materialized table</param>
        /// <returns></returns>
        Task CreateViewTable(SourceView view, string targetSchema);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="table"></param>
        /// <param name="workSchema">To store triggers and key tables</param>
        /// <returns></returns>
        Task EnableChangeTracking(SourceTable table, string workSchema);
        Task UpdateViewTable(ViewSourceTables viewSourceTables, string materializedSchema, string workSchema, CancellationToken stoppingToken);
    }
}