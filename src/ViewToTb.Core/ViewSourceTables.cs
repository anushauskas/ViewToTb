namespace ViewToTb.Core;

public record ViewSourceTables(SourceView View, ICollection<SourceTable> Tables)
{
}
