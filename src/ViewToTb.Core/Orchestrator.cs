using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Linq;

namespace ViewToTb.Core;

internal class Orchestrator
{
    private readonly ILogger<Orchestrator> _logger;
    private readonly IRepository _repository;
    private readonly OrchestratorOptions _options;

    public Orchestrator(ILogger<Orchestrator> logger, IRepository repository, IOptions<OrchestratorOptions> options)
    {
        _logger = logger;
        _repository = repository;
        _options = options.Value;
    }

    public async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        List<ViewSourceTables> viewTables = await Initialize(stoppingToken);

        // Build views dependency trees and sort them
        viewTables = SortByDependenciesCount(viewTables);

        while (!stoppingToken.IsCancellationRequested)
        {
            // Materialize starting with those that have no dependencies
            foreach (var viewSourceTables in viewTables)
            {
                await _repository.UpdateViewTable(viewSourceTables, _options.MaterializedSchema, _options.WorkSchema, stoppingToken);
            }
        }
    }

    private List<ViewSourceTables> SortByDependenciesCount(List<ViewSourceTables> viewTables)
    {
        var viewDependencies = new Dictionary<string, HashSet<string>>();
        var tableDependants = new Dictionary<string, HashSet<string>>();
        var viewRank = new Dictionary<string, int>();
        var viewsQueue = new Queue<string>();
        foreach (var viewSourceTables in viewTables)
        {
            var view = viewSourceTables.View.Name;
            var hasDependency = false;
            viewRank[view] = 0;
            viewDependencies[view] = new HashSet<string>();
            foreach (var table in viewSourceTables.Tables.Where(t => t.Schema.Equals(_options.MaterializedSchema)))
            {
                hasDependency = true;
                if (!tableDependants.ContainsKey(table.Name))
                {
                    tableDependants[table.Name] = new HashSet<string>();
                }
                viewDependencies[view].Add(table.Name);
                tableDependants[table.Name].Add(view);
            }
            if (!hasDependency)
            {
                viewsQueue.Enqueue(view);
            }
        }

        while (viewsQueue.Count > 0)
        {
            var view = viewsQueue.Dequeue();
            foreach (var dependant in tableDependants[view])
            {
                viewRank[dependant] = viewRank[dependant] < viewRank[view] + 1 ? viewRank[view] + 1 : viewRank[dependant] + 1;
                viewsQueue.Enqueue(dependant);
            }
        }

        return viewTables.OrderBy(vt => viewRank[vt.View.Name]).ToList();
    }

    private async Task<List<ViewSourceTables>> Initialize(CancellationToken stoppingToken)
    {
        var views = await _repository.GetViewsAsync(_options.ViewSchema, stoppingToken);
        var viewTables = new List<ViewSourceTables>();
        foreach (var view in views)
        {
            var sourceTables = await _repository.GetSourceTablesAsync(view, stoppingToken);
            viewTables.Add(new ViewSourceTables(view, sourceTables));

            // Should this be done by the orchestrator?
            //await _repository.CreateViewTable(view, _options.MaterializedSchema);
        }

        var allSourceTables = viewTables.SelectMany(vt => vt.Tables).ToHashSet();
        foreach (var sourceTable in allSourceTables)
        {
            await _repository.EnableChangeTracking(sourceTable, _options.WorkSchema);
        }

        return viewTables;
    }
}
