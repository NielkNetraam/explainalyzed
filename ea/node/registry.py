from ea.explainanlyzed_v1 import GenericNode
from ea.node.filter import FilterNode
from ea.node.logical.aggegate import AggregateNode
from ea.node.logical.join import JoinNode
from ea.node.logical.logical_rdd import LogicalRDDNode
from ea.node.logical.relation import RelationNode
from ea.node.physical.file_scan import FileScanNode
from ea.node.plan_node import ColumnarToRowNode, PlanNodeType
from ea.node.project import ProjectNode
from ea.node.union import UnionNode

physical_class_registry: dict[str, type[PlanNodeType]] = {
    "ColumnarToRow": ColumnarToRowNode,
    "FileScan": FileScanNode,
    "Filter": FilterNode,
    "Project": ProjectNode,
    "Union": UnionNode,
    "AdaptiveSparkPlan": GenericNode,
    "Exchange": GenericNode,
}

logical_class_registry: dict[str, type[PlanNodeType]] = {
    "Aggregate": AggregateNode,
    "Filter": FilterNode,
    "Join": JoinNode,
    "LogicalRDD": LogicalRDDNode,
    "Project": ProjectNode,
    "Relation": RelationNode,
    "Union": UnionNode,
}


"""
BaseAggregateExec
HashAggregateExec
ObjectHashAggregateExec
SortAggregateExec

AggregateCodegenSupport
AliasAwareOutputExpression
AliasAwareQueryOutputOrdering
AlterTableExec
AQEShuffleReadExec
AtomicTableWriteExec
BaseCacheTableExec
BaseJoinExec
BaseSubqueryExec
BatchWriteHelper
BatchScanExec
BroadcastExchangeExec
BroadcastExchangeLike
BroadcastHashJoinExec
BroadcastNestedLoopJoinExec
BroadcastQueryStageExec
CacheTableAsSelectExec
CacheTableExec
CoalesceExec
CollectLimitExec
CollectMetricsExec
ColumnarToRowExec
ColumnarToRowTransition
CreateTableAsSelectExec
CodegenSupport
DataSourceScanExec
DataSourceV2ScanExecBase
DataWritingCommandExec
DebugExec
DeleteFromTableExec
DescribeTableExec
DeserializeToObjectExec
DropNamespaceExec
EvalPythonExec
ExecutedCommandExec
ExpandExec
ExternalRDDScanExec
FileSourceScanExec
FilterExec
GenerateExec
HashedRelation
HashJoin
InMemoryTableScanExec
InputAdapter
JoinCodegenSupport
LocalTableScanExec
LongHashedRelation
ObjectConsumerExec
ObjectProducerExec
OrderPreservingUnaryExecNode
OverwriteByExpressionExec
PartitioningPreservingUnaryExecNode
QueryStageExec
RangeExec
ReusedExchangeExec
ReusedSubqueryExec
RowDataSourceScanExec
RowToColumnarExec
SerializeFromObjectExec
SetCatalogAndNamespaceExec
ShowCreateTableExec
ShowTablesExec
ShowTablePropertiesExec
ShuffleExchangeExec
ShuffleExchangeLike
ShuffledHashJoinExec
ShuffledJoin
ShuffleOrigin
ShuffleQueryStageExec
SortMergeJoinExec
SortExec
SparkPlan
SubqueryExec
TableWriteExecHelper
TruncateTableExec
UnaryExecNode
V2CommandExec
V2ExistingTableWriteExec
V2TableWriteExec
WholeStageCodegenExec
WindowExec
WindowExecBase
WriteDeltaExec
WriteFilesExec
"""
