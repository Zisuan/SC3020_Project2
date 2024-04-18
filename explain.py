import psycopg2
import psycopg2.extras
from psycopg2.sql import SQL, Identifier
from psycopg2 import OperationalError

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

def execute_explain(query):
    with DBConnection('TPC-H', 'postgres', 'password', 'localhost', "5432") as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}").format(SQL(query)))
            return cur.fetchone()[0]

def analyze_query(query):
    with DBConnection('TPC-H', 'postgres', 'password', 'localhost', "5432") as conn:
        if conn is None:
            print("Failed to establish a database connection.")
            return None, ""
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            qep = execute_explain(query)
            if qep:
                explained_nodes = set()
                explanation = parse_and_explain(qep[0], cur, explained_nodes)  # Pass cursor here
                return qep, explanation
            else:
                print("Failed to fetch the QEP from the database.")
                return None, ""

def parse_and_explain(qep_json, cursor, explained_nodes):
    explanation = "Query Plan Explanation:\n"
    plan = qep_json['Plan']
    explanation += explain_node(plan, cursor, 0, explained_nodes)
    return explanation

def fetch_stats(cursor, table_name):
    cursor.execute(SQL("SELECT reltuples, relpages FROM pg_class WHERE relname = %s"), (table_name,))
    result = cursor.fetchone()
    return result

def fetch_column_stats(cursor, table_name, column_name):
    cursor.execute(SQL("SELECT n_distinct, avg_width FROM pg_stats WHERE tablename = %s AND attname = %s"), (table_name, column_name))
    result = cursor.fetchone()
    return result

def explain_node(node, cursor, depth=0, explained_nodes=None):
    if explained_nodes is None:
        explained_nodes = set()
    node_identifier = (node.get('Node Type'), node.get('Plan Rows'), node.get('Total Cost'))
    if node_identifier in explained_nodes:
        return ''
    explained_nodes.add(node_identifier)
    node_type = node.get('Node Type', 'Unknown')
    table_name = node.get('Relation Name', None)
    column_name = node.get('Index Name', None)  # Or derive from node['Index Cond'] or similar
    explanation = "    " * depth + f"Node Type: {node_type}\n"

    if table_name:
        stats = fetch_stats(cursor, table_name)
        if stats:
            explanation += "    " * depth + f"Table '{table_name}' stats: {stats['reltuples']} rows, {stats['relpages']} pages.\n"

    if table_name and column_name:
        col_stats = fetch_column_stats(cursor, table_name, column_name)
        if col_stats:
            explanation += "    " * depth + f"Column '{column_name}' stats: {col_stats['n_distinct']} distinct values, avg width {col_stats['avg_width']}.\n"

    explanation += "    " * depth + f"Cost: Startup {node.get('Startup Cost')}, Total {node.get('Total Cost')}\n"
    explanation += "    " * depth + f"Rows: {node.get('Plan Rows')}, Width: {node.get('Plan Width')}\n"

    # Recursively explain child nodes
    if 'Plans' in node:
        for child in node['Plans']:
            explanation += explain_node(child, cursor, depth + 1, explained_nodes)
    return explanation

if __name__ == '__main__':
    query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey;"
    qep_json, explanation = analyze_query(query)
    print(explanation)