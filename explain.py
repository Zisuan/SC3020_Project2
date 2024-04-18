import psycopg2
import psycopg2.extras
from psycopg2.sql import SQL, Identifier
from psycopg2 import OperationalError

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
                return f"{indent}Table '{table_name}' stats: {stats['reltuples']} rows, {stats['relpages']} pages.\n"
        return ""

@register_node('Index Scan')
class IndexScanNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        index_name = self.node_json.get('Index Name')
        table_name = self.node_json.get('Relation Name')
        if index_name and table_name:
            self.cursor.execute("SELECT idx_scan, idx_tup_read FROM pg_stat_user_indexes WHERE indexrelname = %s", (index_name,))
            if index_stats:
                index_stats = self.cursor.fetchone()
            return f"{indent}Index '{index_name}' on table '{table_name}' stats: scans {index_stats['idx_scan']}, tuples read {index_stats['idx_tup_read']}.\n"
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
                return f"{indent}    Index '{index_name}' stats: scans {index_stats['idx_scan']}, tuples read {index_stats['idx_tup_read']}.\n"
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
                return f"{indent}    Table '{table_name}' stats: {stats['reltuples']} rows, {stats['relpages']} pages.\n"
        return ""

@register_node('Nested Loop Join')
class NestedLoopJoinNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        # Example of adding join condition details
        join_condition = self.node_json.get('Join Filter', 'No specific join condition reported')
        return f"{indent}    Nested Loop Join uses condition: {join_condition}\n"

@register_node('Merge Join')
class MergeJoinNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        sort_keys = self.node_json.get('Merge Key', 'No merge keys reported')
        return f"{indent}    Merge Join on keys: {sort_keys}\n"

@register_node('Hash')
class HashNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        # Hypothetical memory usage statistics
        memory_usage = self.node_json.get('Memory Usage', 'Memory details not provided')
        return f"{indent}    Hash operation with memory usage: {memory_usage}\n"

@register_node('Hash Join')
class HashJoinNode(Node):
    def fetch_stats(self, depth):
        indent = '    ' * depth
        hash_conditions = self.node_json.get('Hash Cond', 'No hash condition reported')
        return f"{indent}    Hash Join uses condition: {hash_conditions}\n"

# Execution and explanation parsing
def execute_explain(query):
    with DBConnection('TPC-H', 'postgres', 'password', 'localhost', "5432") as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}").format(SQL(query)))
            return cur.fetchone()[0]

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

def analyze_query(query):
    with DBConnection('TPC-H', 'postgres', 'password', 'localhost', "5432") as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            qep = execute_explain(query)
            if qep:
                explanation = "Query Plan Explanation:\n" + parse_and_explain(qep[0], cur)
                return qep, explanation
            else:
                print("Failed to fetch the QEP from the database.")
                return None, ""

if __name__ == '__main__':
    query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey;"
    qep_json, explanation = analyze_query(query)
    print(explanation)