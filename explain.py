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
        explanation += f"{indent}Estimated Cost: Startup {self.node_json.get('Startup Cost')},Total {self.node_json.get('Total Cost')}\n"
        explanation += self.fetch_stats(depth)
        for child in self.children:
            explanation += child.explain(depth + 1)
        return explanation
    
    def fetch_stats(self, depth):
        return ""

class ScanNodes(Node):

    def fetch_stats(self, depth):
        indent = '    ' * depth
        stats_info = super().fetch_stats(depth)  # Call to base class fetch_stats if needed

        num_tuples = self.cardinality(True)
        num_blocks = self.cardinality(False)
        
        # Building stats information string
        stats_info += f"{indent}Estimated Tuples: {num_tuples}, Estimated Blocks: {num_blocks}\n"
        stats_info += f"{indent}Filter/Condition: {self.node_json.get('Filter', self.node_json.get('Index Cond', 'None'))}\n"
        return stats_info

    def cardinality(self, is_tuple):
        rel = self.node_json.get("Relation Name", "unknown")
        self.cursor.execute("SELECT reltuples, relpages FROM pg_class WHERE relname = %s", (rel,))
        stats = self.cursor.fetchone()
        if not stats:
            return 0

        num_blocks = stats['relpages']
        num_tuples = stats['reltuples']

        if "Filter" not in self.node_json and "Index Cond" not in self.node_json:
            return num_tuples if is_tuple else num_blocks

        selectivity = self.estimate_selectivity()
        return int(num_tuples * selectivity) if is_tuple else int(num_blocks * selectivity)

    def estimate_selectivity(self):
        if "Filter" in self.node_json:
            conditions = self.node_json["Filter"]
        elif "Index Cond" in self.node_json:
            conditions = self.node_json["Index Cond"]
        else:
            return 1  # No condition selectivity = 1

        attr = self.retrieve_attribute_from_condition()
        op = self.retrieve_operator_from_condition()

        self.cursor.execute(f"SELECT COUNT(DISTINCT {attr}) FROM {self.node_json.get('Relation Name')}")
        num_unique = self.cursor.fetchone()[0]

        if ">" in op or "<" in op:
            return 1 / 3
        elif num_unique == 0:
            return 0
        else:
            return 1 / num_unique

    def retrieve_attribute_from_condition(self):
        condition = self.node_json.get("Filter") or self.node_json.get("Index Cond")
        if condition:
            return condition.split()[0].strip("()").split(".")[-1]
        return None

    def retrieve_operator_from_condition(self):
        condition = self.node_json.get("Filter") or self.node_json.get("Index Cond")
        if condition:
            return condition.split()[1]
        return None

# Specific Node implementations
@register_node('Seq Scan')
class SeqScanNode(ScanNodes):  # Inherit from ScanNodes
    def fetch_stats(self, depth):
        indent = '    ' * depth
        table_name = self.node_json.get('Relation Name')
        if table_name:
            self.cursor.execute("SELECT reltuples, relpages FROM pg_class WHERE relname = %s", (table_name,))
            stats = self.cursor.fetchone()
            if stats:
                manual_cost = stats['relpages']  # Simplified cost calculation based on page count
                dbms_estimated_cost = self.node_json.get('Total Cost')

                explanation = super().fetch_stats(depth)  # Call to modified fetch_stats from ScanNodes
                explanation += f"{indent}Manual Cost Formula: B(R) = {manual_cost}\n"
                explanation += f"{indent}Calculated Cost: {manual_cost} (Estimated Cost by DBMS: {dbms_estimated_cost})\n"
                
                if manual_cost != dbms_estimated_cost:
                    explanation += f"{indent}Difference Explanation: PostgreSQL factors in efficiencies not captured here.\n"
                
                return explanation
        return ""

@register_node('Index Scan')
class IndexScanNode(ScanNodes):  # Inherit from ScanNodes
    def fetch_stats(self, depth):
        indent = '    ' * depth
        index_name = self.node_json.get('Index Name')
        table_name = self.node_json.get('Relation Name')
        if index_name and table_name:
            self.cursor.execute("SELECT idx_scan, idx_tup_read FROM pg_stat_user_indexes WHERE indexrelname = %s", (index_name,))
            index_stats = self.cursor.fetchone()
            if index_stats:
                manual_cost = index_stats['idx_tup_read']  # Simplified cost calculation based on tuple reads
                estimated_cost = self.node_json.get('Total Cost')

                explanation = super().fetch_stats(depth)  # Call to modified fetch_stats from ScanNodes
                explanation += f"{indent}Manual Cost Formula: T(R) / V(R, a) = {manual_cost}\n"
                explanation += f"{indent}Calculated Cost: {manual_cost} (Estimated Cost by DBMS: {estimated_cost})\n"
                
                if manual_cost != estimated_cost:
                    explanation += f"{indent}Difference Explanation: Factors like index selectivity and disk I/O are optimized by PostgreSQL.\n"
                
                return explanation
        return ""

    def calculate_cost(self, table_name, index_name):
        # Fetch total tuples (T(R)) and unique values (V(R, a))
        self.cursor.execute("SELECT reltuples FROM pg_class WHERE relname = %s", (table_name,))
        total_tuples = self.cursor.fetchone()[0]
        self.cursor.execute("SELECT count(DISTINCT column_name) FROM table_name WHERE index_name = %s", (index_name,))
        unique_values = self.cursor.fetchone()[0]
        return total_tuples / unique_values if unique_values else total_tuples  # Protect against division by zero if no unique values

@register_node('Nested Loop Join') # done
class NestedLoopJoinNode(ScanNodes): 
    def fetch_stats(self, depth):
        indent = '    ' * depth
        base_stats = super().fetch_stats(depth)
        left_child = self.node_json.get('Plans', [])[0] if 'Plans' in self.node_json else {}
        right_child = self.node_json.get('Plans', [])[1] if len(self.node_json.get('Plans', [])) > 1 else {}
        left_cost = left_child.get('Total Cost', 0)
        right_cost = right_child.get('Total Cost', 0)
        left_rows = left_child.get('Plan Rows', 0)
        manual_cost = left_cost + (left_rows * right_cost)
        estimated_cost = self.node_json.get('Total Cost')
        explanation = f"{indent}Nested Loop Join uses condition: {self.node_json.get('Join Filter', 'No specific join condition reported')}\n"
        explanation += f"{indent}Manual Cost Formula: Outer Loop Cost(Total Cost of R) + (Outer Loop Rows(Rows in R) × Inner Loop Cost(Total Cost of S)) = {manual_cost}\n"
        explanation += f"{indent}Written in simplier terms, Manual Cost Formula: min(B(R), B(S)) + (B(R) * B(S))\n"
        explanation += f"{indent}Calculated Cost: {manual_cost} (Estimated Cost by DBMS: {estimated_cost})\n"
        if manual_cost != estimated_cost:
            explanation += f"{indent}Difference Explanation: PostgreSQL may optimize nested loop joins by using indexing on the inner relation or caching the inner relation in memory if it is small enough. These optimizations can significantly reduce the actual cost compared to the manual estimation, especially if the inner relation is accessed multiple times.\n"
            explanation += f"{indent}PostgreSQL also considers the cost of handling tuples that meet the join condition and may benefit from tuple prefetching and other join algorithms when applicable.\n"
        return base_stats + explanation

@register_node('Merge Join') # done
class MergeJoinNode(ScanNodes):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        base_stats = super().fetch_stats(depth)
        left_child = self.node_json.get('Plans', [])[0] if 'Plans' in self.node_json else {}
        right_child = self.node_json.get('Plans', [])[1] if len(self.node_json.get('Plans', [])) > 1 else {}
        left_cost = left_child.get('Total Cost', 0)
        right_cost = right_child.get('Total Cost', 0)
        manual_cost = 3 * (left_cost + right_cost)
        estimated_cost = self.node_json.get('Total Cost')

        explanation = f"{indent}Merge Join on keys: {self.node_json.get('Merge Key', 'No merge keys reported')}\n"
        explanation += f"{indent}Manual Cost Formula: 3(B(R) + B(S)) = {manual_cost}\n"
        explanation += f"{indent}Calculated Cost: {manual_cost} (Estimated Cost by DBMS: {estimated_cost})\n"
        if manual_cost != estimated_cost:
            explanation += f"{indent}Difference Explanation: PostgreSQL's optimizer might choose this join for its efficiency in certain sorted datasets, a nuance not captured by the simple manual cost.\n"
        return base_stats + explanation

@register_node('Hash') # No formula provided by course
class HashNode(ScanNodes):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        base_stats = super().fetch_stats(depth)
        estimated_rows = self.node_json.get('Plan Rows', 0)
        estimated_cost = self.node_json.get('Total Cost')
        explanation = f"{indent}Hash operation involves approximately {estimated_rows} rows.\n"
        explanation += f"{indent}Manual Cost Formula not available\n"
        explanation += f"{indent}Estimated Cost by DBMS: {estimated_cost}\n"
        #explanation += f"{indent}Difference Explanation: Actual hash costs in PostgreSQL also consider factors such as hash bucket density and memory availability.\n"
        return base_stats + explanation

@register_node('Hash Join') # done
class HashJoinNode(ScanNodes):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        base_stats = super().fetch_stats(depth)
        left_child = self.node_json['Plans'][0]
        right_child = self.node_json['Plans'][1]
        R_block_size = left_child.get('Plan Rows', 0)
        S_block_size = right_child.get('Plan Rows', 0)
        manual_cost = 3 * (R_block_size + S_block_size)
        estimated_cost = self.node_json.get('Total Cost')
        explanation = f"{indent}Hash Join uses condition: {self.node_json.get('Hash Cond', 'No hash condition reported')}\n"
        explanation += f"{indent}Manual Cost Formula: 3(B(R) + B(S)) = {manual_cost}\n"
        explanation += f"{indent}Calculated Cost: {manual_cost} (Estimated Cost by DBMS: {estimated_cost})\n"
        if manual_cost != estimated_cost:
            explanation += f"{indent}Difference Explanation: PostgreSQL may optimize hash joins with in-memory hash tables, which can significantly alter the real-world costs, not shown here.\n"
        return base_stats + explanation

@register_node('Gather')  # No formula provided by course
@register_node('Gather')
class GatherNode(ScanNodes):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        base_stats = super().fetch_stats(depth)
        estimated_cost = self.node_json.get('Total Cost')
        explanation = f"{indent}Gather node combines the output of child nodes executed by parallel workers.\n"
        explanation += f"{indent}Manual Cost Formula not available\n"
        explanation += f"Estimated Cost by DBMS: {estimated_cost}\n"
        return base_stats + explanation

@register_node('Gather Merge')  # No formula provided by course
class GatherMergeNode(ScanNodes):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        base_stats = super().fetch_stats(depth)
        estimated_cost = self.node_json.get('Total Cost')
        explanation = f"{indent}Gather Merge combines sorted outputs of parallel workers preserving the order.\n"
        explanation += f"{indent}Manual Cost Formula not available\n"
        explanation += f"Estimated Cost by DBMS: {estimated_cost}\n"
        #explanation += f"{indent}Difference Explanation: PostgreSQL uses a heap that at any instant holds the next tuple from each stream, which can affect performance depending on the size of streams.\n"
        return base_stats + explanation

@register_node('Sort') # Done
class SortNode(ScanNodes):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        base_stats = super().fetch_stats(depth)
        rel_name = self.node_json.get('Relation Name')
        if rel_name:
            self.cursor.execute("SELECT relpages FROM pg_class WHERE relname = %s", (rel_name,))
            result = self.cursor.fetchone()
            if result:
                num_blocks = result[0]
                manual_cost = 3 * num_blocks
                estimated_cost = self.node_json.get('Total Cost')
                explanation = f"{indent}Sort on relation: {rel_name}.\n"
                explanation += f"{indent}Manual Cost Formula: 3B = {manual_cost}\n"
                explanation += f"{indent}Calculated Cost: {manual_cost} (Estimated Cost by DBMS: {estimated_cost})\n"
                if manual_cost != estimated_cost:
                    explanation += f"{indent}Difference Explanation: PostgreSQL may adjust costs based on work memory and actual data size which are not factored into manual calculations.\n"
                    return base_stats + explanation
        return f"{indent}Unable to retrieve block information for relation: {rel_name}\n" + base_stats

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