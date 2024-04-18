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
        self.conn = None #Initialize connection to None

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )

        except OperationalError as e:
            print(f"An error occurred while connecting to the database: {e}")
        return self.conn    #This may return None if connection fails

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

def execute_explain(query):
    with DBConnection('TPC-H', 'postgres', 'password', 'localhost', "5432") as conn:
        if conn is None:
            print("Failed to establish a database connection.")
            return None
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}").format(SQL(query)))
            return cur.fetchone()[0]

def analyze_query(query):
    qep = execute_explain(query)
    if qep:
        explained_nodes = set()
        explanation = parse_and_explain(qep, explained_nodes)
        return qep, explanation
    else:
        print("Failed to fetch the QEP from the database.")
        return None, ""

def parse_and_explain(qep_json, explained_nodes):
    explanation = "Query Plan Explanation:\n"
    for qep_part in qep_json:
        plan = qep_part['Plan']
        explanation += explain_node(plan, 0, explained_nodes)
    return explanation

def explain_node(node, depth=0, explained_nodes=None):
    if explained_nodes is None:
        explained_nodes = set()
    node_identifier = (node.get('Node Type'), node.get('Plan Rows'), node.get('Total Cost'))
    if node_identifier in explained_nodes:
        return ''
    explained_nodes.add(node_identifier)
    node_type = node.get('Node Type', 'Unknown')
    startup_cost = node.get('Startup Cost', 'Unknown')
    total_cost = node.get('Total Cost', 'Unknown')
    rows = node.get('Plan Rows', 'Unknown')
    width = node.get('Plan Width', 'Unknown')
    explanation = "------------------\n"
    explanation += f"Node Type: {node_type} (Cost: {startup_cost}..{total_cost} rows={rows} width={width})\n"
    # Append explanations based on node_type...
    if 'Plans' in node:
        for child in node['Plans']:
            explanation += explain_node(child, depth + 1, explained_nodes)
    return explanation

if __name__ == '__main__':
    query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey;"
    qep_json, explanation = analyze_query(query)
    print(explanation)
