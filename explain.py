import psycopg2
import psycopg2.extras
from psycopg2.sql import SQL, Identifier
from psycopg2.extras import DictCursor

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
    with conn.cursor(cursor_factory=DictCursor) as cur:  
        qep = execute_explain(query, cur)
        if qep:
            explanation = "Query Plan Explanation:\n" + parse_and_explain(qep[0], cur)
            return qep, explanation
        else:
            print("Failed to fetch the QEP from the database.")
            return None, ""

if __name__ == '__main__':
    with DBConnection('TPC-H', 'postgres', 'password', 'localhost', "5432") as conn:
        query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey;"
        qep_json, explanation = analyze_query(query, conn)
        print(explanation)