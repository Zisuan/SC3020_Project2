import psycopg2
import psycopg2.extras
from psycopg2.sql import SQL, Identifier
from psycopg2.extras import DictCursor
from math import log2

# Database connection management
class DBConnection:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

# Node registration system
node_registry = {}

def register_node(node_type):
    def decorator(cls):
        node_registry[node_type] = cls
        return cls
    return decorator

# Base Node class
class Node:
    def __init__(self, node_json, cursor):
        self.node_json = node_json
        self.cursor = cursor
        self.children = []

    def fetch_stats(self):
        # This method will be overridden in child classes if needed.
        return ""

    def explain(self, depth=0):
        indent = '    ' * depth
        explanation = f"{indent}Node Type: {self.node_json.get('Node Type', 'Unknown')}\n"
        explanation += f"{indent}Cost: Startup {self.node_json.get('Startup Cost')}, Total {self.node_json.get('Total Cost')}\n"
        explanation += f"{indent}Rows: {self.node_json.get('Plan Rows')}, Width: {self.node_json.get('Plan Width')}\n"
        explanation += self.fetch_stats(depth)
        for child in self.children:
            explanation += child.explain(depth + 1)
        return explanation
    
    def fetch_stats(self, depth):
        return ""

# Specific Node implementations
@register_node('Seq Scan')
class SeqScanNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        table_name = self.node_json.get('Relation Name')
        if table_name:
            self.cursor.execute("SELECT reltuples, relpages FROM pg_class WHERE relname = %s", (table_name,))
            stats = self.cursor.fetchone()
            if stats:
                num_pages = stats['relpages']
                manual_cost = num_pages  # Assuming cost per page is 1 for simplicity
                explanation = f"{indent}Table '{table_name}' stats: {stats['reltuples']} rows, {num_pages} pages.\n"
                explanation += f"{indent}Manual Cost Formula: B(R) = {manual_cost}\n"
                explanation += f"{indent}Difference Explanation: PostgreSQL factors in parallel CPU processing making in more efficient in estimations.\n"
                return explanation
        return ""

@register_node('Index Scan')
class IndexScanNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        index_name = self.node_json.get('Index Name')
        table_name = self.node_json.get('Relation Name')
        if index_name and table_name:
            self.cursor.execute("SELECT idx_scan, idx_tup_read FROM pg_stat_user_indexes WHERE indexrelname = %s", (index_name,))
            index_stats = self.cursor.fetchone()
            if index_stats:
                num_scans = index_stats['idx_scan']
                num_tuples_read = index_stats['idx_tup_read']
                manual_cost = num_tuples_read  # Assuming cost per tuple read is 1 for simplicity
                explanation = f"{indent}Index '{index_name}' on table '{table_name}' stats: scans {num_scans}, tuples read {num_tuples_read}.\n"
                explanation += f"{indent}Manual Cost Formula: T(R) / V(R, a) = {manual_cost}\n"
                explanation += f"{indent}Difference Explaination: PostgreSQL optimizes index scans with cost estimates based on index selectivity, which may not be fully captured here\n"
                return explanation
        return ""

@register_node('Bitmap Index Scan')
class BitmapIndexScanNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        index_name = self.node_json.get('Index Name')
        if index_name:
            self.cursor.execute("SELECT idx_scan, idx_tup_read FROM pg_stat_user_indexes WHERE indexrelname = %s", (index_name,))
            index_stats = self.cursor.fetchone()
            if index_stats:
                manual_cost = index_stats['idx_scan']  # Simplified example
                explanation = f"{indent}Index '{index_name}' stats: scans {index_stats['idx_scan']}, tuples read {index_stats['idx_tup_read']}.\n"
                explanation += f"{indent}Manual Cost Formula: Cost is based on number of scans: {manual_cost}\n"
                explanation += f"{indent}Differece Explanation: PostgreSQL might optimize this by batching and combining index scans for efficiency, not reflected in this simple model\n"
                return explanation
        return ""

@register_node('Bitmap Heap Scan')
class BitmapHeapScanNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        table_name = self.node_json.get('Relation Name')
        if table_name:
            self.cursor.execute("SELECT reltuples, relpages FROM pg_class WHERE relname = %s", (table_name,))
            stats = self.cursor.fetchone()
            if stats:
                num_pages = stats['relpages']
                manual_cost = num_pages * 0.1  # Assume each page cost is reduced by bitmap index efficiency
                explanation = f"{indent}Table '{table_name}' stats: {stats['reltuples']} rows, {num_pages} pages.\n"
                explanation += f"{indent}Manual Cost Formula: Bitmap Heap Scan Cost = Pages * Reduced Cost/Page = {manual_cost}\n"
                explanation += f"{indent}Differece Explanation: PostgreSQL execution might use bloom filters or other structures to further reduce page access, not accounted here\n"
                return explanation
        return ""

@register_node('Nested Loop Join')
class NestedLoopJoinNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        left_child = self.node_json.get('Plans', [])[0] if 'Plans' in self.node_json else {}
        right_child = self.node_json.get('Plans', [])[1] if len(self.node_json.get('Plans', [])) > 1 else {}
        left_cost = left_child.get('Total Cost', 0)
        right_cost = right_child.get('Total Cost', 0)
        manual_cost = left_cost + (left_child.get('Plan Rows', 0) * right_cost)
        explanation = f"{indent}Nested Loop Join uses condition: {self.node_json.get('Join Filter', 'No specific join condition reported')}\n"
        explanation += f"{indent}Manual Cost Formula: Outer Loop Cost + (Outer Loop Rows × Inner Loop Cost) = {manual_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL may optimize nested loop joins by using indexing on the inner relation or caching the inner relation in memory if it is small enough. These optimizations can significantly reduce the actual cost compared to the manual estimation, especially if the inner relation is accessed multiple times.\n"
        explanation += f"{indent}PostgreSQL also considers the cost of handling tuples that meet the join condition and may benefit from tuple prefetching and other join algorithms when applicable.\n"
        return explanation

@register_node('Merge Join')
class MergeJoinNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        left_child = self.node_json.get('Plans', [])[0] if 'Plans' in self.node_json else {}
        right_child = self.node_json.get('Plans', [])[1] if len(self.node_json.get('Plans', [])) > 1 else {}
        left_cost = left_child.get('Total Cost', 0)
        right_cost = right_child.get('Total Cost', 0)
        manual_cost = (left_cost + right_cost) * 1.5  # Assume some additional cost for merging
        explanation = f"{indent}Merge Join on keys: {self.node_json.get('Merge Key', 'No merge keys reported')}\n"
        explanation += f"{indent}Manual Cost Formula: (Sort Cost of Left Side + Sort Cost of Right Side + Merge Cost) = {manual_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL's optimizer might choose this join for its efficiency in certain sorted datasets, a nuance not captured by the simple manual cost.\n"
        return explanation

@register_node('Hash')
class HashNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        # Example of assuming memory cost per hashed row
        estimated_rows = self.node_json.get('Plan Rows', 0)
        manual_cost = estimated_rows * 0.5  # Assume some cost per hashed row
        explanation = f"{indent}Hash operation involves approximately {estimated_rows} rows.\n"
        explanation += f"{indent}Manual Cost Formula: Hash Cost = Estimated Rows * Cost per row = {manual_cost}\n"
        explanation += f"{indent}Difference Explanation: Actual hash costs in PostgreSQL also consider factors such as hash bucket density and memory availability.\n"
        return explanation

@register_node('Hash Join')
class HashJoinNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        left_child = self.node_json['Plans'][0]  # Assuming left child exists
        right_child = self.node_json['Plans'][1]  # Assuming right child exists
        left_cost = 3 * left_child['Total Cost']  # Simplified example
        right_cost = 3 * right_child['Total Cost']  # Simplified example
        manual_cost = left_cost + right_cost
        explanation = f"{indent}Hash Join uses condition: {self.node_json.get('Hash Cond', 'No hash condition reported')}\n"
        explanation += f"{indent}Manual Cost Formula: 3(B(R) + B(S)) = {manual_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL may optimize hash joins with in-memory hash tables, which can significantly alter the real-world costs, not shown here.\n"
        return explanation

@register_node('Gather')
class GatherNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        total_cost = sum(child.get('Total Cost', 0) for child in self.node_json.get('Plans', []))
        explanation = f"{indent}Gather node combines the output of child nodes executed by parallel workers.\n"
        explanation += f"{indent}Manual Cost Formula: Sum of children costs = {total_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL's implementation may include additional parallel setup and communication costs.\n"
        return explanation

@register_node('Gather Merge')
class GatherMergeNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        child_costs = [child.get('Total Cost', 0) for child in self.node_json.get('Plans', [])]
        total_cost = sum(child_costs)
        merge_cost = len(child_costs) * log2(len(child_costs)) if child_costs else 0
        total_cost += merge_cost
        explanation = f"{indent}Gather Merge combines sorted outputs of parallel workers preserving the order.\n"
        explanation += f"{indent}Manual Cost Formula: Sum of child costs + Merge cost = {total_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL uses a heap that at any instant holds the next tuple from each stream, which can affect performance depending on the size of streams.\n"
        return explanation

@register_node('Sort')
class SortNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        rel_name = self.node_json.get('Relation Name')
        sort_method = self.node_json.get('Sort Method', 'unknown')
        manual_cost = {
            'external merge': lambda x: x * 3,
            'quicksort': lambda x: x,
            'top-N heapsort': lambda x: x / 3
        }.get(sort_method, lambda x: x)(self.node_json.get('Total Cost', 0))
        explanation = f"{indent}Sort on relation: {rel_name} using {sort_method}.\n"
        explanation += f"{indent}Manual Cost Formula: {sort_method} cost = {manual_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL may adjust costs based on work memory and actual data size which are not factored into manual calculations.\n"
        return explanation

@register_node('Incremental Sort')
class IncrementalSortNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        relation_name = self.node_json.get('Relation Name')
        # Example SQL query to fetch total and estimated sorted blocks
        self.cursor.execute("SELECT total_blocks, estimated_sorted_blocks FROM some_table WHERE relation_name = %s", (relation_name,))
        data = self.cursor.fetchone()
        total_blocks = data['total_blocks']
        estimated_sorted_blocks = data['estimated_sorted_blocks']
        manual_cost = total_blocks - estimated_sorted_blocks
        explanation = f"{indent}Incremental sort on {relation_name} with partially ordered input.\n"
        explanation += f"{indent}Manual Cost Formula: B(rel) - Estimated Sorted Blocks = {manual_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL uses different calculations for the number of groups with equal presorted keys.\n"
        return explanation

@register_node('Limit')
class LimitNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        relation_name = self.node_json.get('Relation Name')
        # Example SQL query to fetch block counts
        self.cursor.execute("SELECT block_count FROM some_table WHERE relation_name = %s", (relation_name,))
        block_count = self.cursor.fetchone()['block_count']
        manual_cost = block_count
        explanation = f"{indent}Limit operation on {relation_name}.\n"
        explanation += f"{indent}Manual Cost Formula: B(rel) = {manual_cost}\n"
        explanation += f"{indent}Difference Explanation: In reality, PostgreSQL will process fewer rows due to the limit clause.\n"
        return explanation

@register_node('Materialize')
class MaterializeNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        relation_name = self.node_json.get('Relation Name')
        # SQL query to fetch tuple counts
        self.cursor.execute("SELECT tuple_count FROM some_table WHERE relation_name = %s", (relation_name,))
        tuple_count = self.cursor.fetchone()['tuple_count']
        manual_cost = tuple_count * 2
        explanation = f"{indent}Materialize intermediate results for {relation_name}.\n"
        explanation += f"{indent}Manual Cost Formula: T(rel) * 2 = {manual_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL includes CPU and I/O costs for writing to and reading from temporary storage.\n"
        return explanation

@register_node('Memoize')
class MemoizeNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        explanation = f"{indent}Memoize reuses results of expensive suboperations.\n"
        explanation += f"{indent}Manual Cost Formula: 0 (No additional cost for cached results)\n"
        explanation += f"{indent}Difference Explanation: Costs depend on operation frequency and cache hit rate.\n"
        return explanation

@register_node('Aggregate')
class AggregateNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        relation_name = self.node_json.get('Relation Name')
        # Fetch the total tuples and the count of unique groups (assuming group key defined)
        self.cursor.execute("SELECT COUNT(*), COUNT(DISTINCT group_key) FROM " + relation_name)
        result = self.cursor.fetchone()
        total_tuples = result[0]
        unique_groups = result[1]
        if self.node_json.get("Strategy") == "Hashed":
            manual_cost = total_tuples  # Assuming a simplified model for hashed aggregation
        else:
            manual_cost = total_tuples * unique_groups
        explanation = f"{indent}Aggregate operation on {relation_name}.\n"
        explanation += f"{indent}Manual Cost Formula: T(rel) * Number of Groups = {manual_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL uses optimized aggregation strategies that might affect performance differently.\n"
        return explanation

@register_node('Group')
class GroupNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        relation_name = self.node_json.get('Relation Name')
        # SQL query to get the count of distinct group keys
        self.cursor.execute("SELECT COUNT(DISTINCT group_key) FROM " + relation_name)
        num_groups = self.cursor.fetchone()[0]
        self.cursor.execute("SELECT COUNT(*) FROM " + relation_name)
        num_tuples = self.cursor.fetchone()[0]
        manual_cost = num_tuples * num_groups
        explanation = f"{indent}Group by operation on {relation_name}.\n"
        explanation += f"{indent}Manual Cost Formula: T(rel) * Number of Group Columns = {manual_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL incorporates advanced optimizations for group by operations.\n"
        return explanation

@register_node('Unique')
class UniqueNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        relation_name = self.node_json.get('Relation Name')
        # Example SQL query to find the number of unique tuples
        self.cursor.execute("SELECT COUNT(DISTINCT *) FROM " + relation_name)
        unique_count = self.cursor.fetchone()[0]
        manual_cost = unique_count  # Simplified assumption
        explanation = f"{indent}Unique operation to remove duplicates from {relation_name}.\n"
        explanation += f"{indent}Manual Cost Formula: Number of Unique Rows = {manual_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL may use different strategies for de-duplication based on the query plan.\n"
        return explanation

@register_node('Bitmap And')
class BitmapAndNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        explanation = f"{indent}Bitmap AND operation is generally negligible in cost.\n"
        explanation += f"{indent}Manual Cost Formula: Negligible.\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL manages bitmap operations in memory, rarely requiring additional computation.\n"
        return explanation

@register_node('Bitmap Or')
class BitmapOrNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        explanation = f"{indent}Bitmap OR operation is generally negligible in cost.\n"
        explanation += f"{indent}Manual Cost Formula: Negligible.\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL processes these operations with minimal overhead.\n"
        return explanation

@register_node('Append')
class AppendNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        child_costs = [child.node_json.get('Total Cost', 0) for child in self.children]
        total_cost = sum(child_costs)
        explanation = f"{indent}Append Node combines results of sub-plans.\n"
        explanation += f"{indent}Manual Cost Formula: Sum of child costs = {total_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL dynamically computes the cost based on child plans, possibly including additional overhead.\n"
        return explanation

@register_node('Merge Append')
class MergeAppendNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        child_costs = [child.node_json.get('Total Cost', 0) for child in self.children]
        total_cost = sum(child_costs)
        merge_cost = len(self.children) * log2(len(self.children)) if self.children else 0
        total_cost += merge_cost
        explanation = f"{indent}Merge Append Node combines sorted results of child operations, preserving their order. Includes cost of merging.\n"
        explanation += f"{indent}Manual Cost Formula: Sum of child costs + Merge cost = {total_cost}\n"
        explanation += f"{indent}Difference Explanation: PostgreSQL also considers the complexity of maintaining heap structures during merging.\n"
        return explanation

# Execution and explanation parsing
def execute_explain(query, cursor):
    cursor.execute(SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}").format(SQL(query)))
    return cursor.fetchone()[0]

def parse_and_explain(qep_json, cursor):
    def create_node(plan, cursor):
        node_type = plan['Node Type']
        return node_registry.get(node_type, Node)(plan, cursor)

    root_node = create_node(qep_json['Plan'], cursor)
    stack = [(root_node, qep_json['Plan'])]
    while stack:
        node, plan = stack.pop()
        if 'Plans' in plan:
            for subplan in plan['Plans']:
                child_node = create_node(subplan, cursor)
                node.children.append(child_node)
                stack.append((child_node, subplan))
    return root_node.explain()

def analyze_query(query, conn):
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            qep = execute_explain(query, cur)
            if qep:
                explanation = "Query Plan Explanation:\n" + parse_and_explain(qep[0], cur)
                return qep, explanation
    except psycopg2.DatabaseError as db_err:
        return None, f"Database error: {db_err}"
    except psycopg2.ProgrammingError as pg_err:
        return None, f"Programming error: {pg_err}"
    except Exception as e:
        return None, f"An error occurred: {e}"


if __name__ == '__main__':
    with DBConnection('TPC-H', 'postgres', 'password', 'localhost', "5432") as conn:
        query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey;"
        qep_json, explanation = analyze_query(query, conn)
        print(explanation)