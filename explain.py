import psycopg2
import psycopg2.extras
import json

def connect_to_db():
    # Create a connection to the PostgreSQL database.
    try:
        conn = psycopg2.connect(
            dbname='TPC-H',
            user='postgres',
            password='password',
            host='localhost',
            port="5432",
        )
        return conn
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")
        return None

def execute_explain(query):
    # Execute the EXPLAIN command for a given SQL query and return the plan in JSON format.
    conn = connect_to_db()
    if conn is not None:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
                explain_result = cur.fetchone()
                conn.close()
                return explain_result[0]
        except Exception as e:
            print(f"An error occurred while executing EXPLAIN: {e}")
            return None
    else:
        return None

def analyze_query(query):
    # Analyze the given SQL query, returning a QEP and its cost explanation.
    qep = execute_explain(query)
    if qep is not None:
        explanation = parse_and_explain(qep)
        return qep, explanation
    else:
        print("Failed to fetch the QEP from the database.")
        return None 

def parse_and_explain(qep):
    # Parse the QEP and generate a explanation of cost estimations.
    # This is a placeholder for the logic to parse the QEP and explain costs.
    # The complexity of this function will vary depending on how detailed you want the explanation to be.
    # As a starting point, focus on identifying key operations (e.g., Seq Scan, Index Scan) and their costs.
    
    explanation = "Query Plan Explanation:\n"
    # Since the top-level of JSON is a list, iterate through each element (plan).
    for plan in qep:
        explanation += explain_node(plan['Plan'])  # Adjusted to access the 'Plan' key directly
    return explanation


def explain_node(node, depth=0):
    # Recursively explain a single node in the QEP.
    # Placeholder for explanation logic for a single node.
    # You might want to detail the type of scan, estimated rows, cost, etc.
    node_type = node.get('Node Type', 'Unknown')
    total_cost = node.get('Total Cost', 'Unknown')  # Use 'get' to avoid KeyError if the key is absent
    explanation = "\t" * depth + f"Node Type: {node_type}, Cost: {total_cost}\n"
    
    # Check and recursively explain child nodes if they exist
    if 'Plans' in node:
        for child in node['Plans']:  # 'Plans' is expected to be a list of child plans
            explanation += explain_node(child, depth + 1)
    return explanation

if __name__ == '__main__':
    # Example usage for testing
    query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey AND O.o_orderstatus='F' AND O.o_shippriority = 0 AND C.c_custkey < 500;"
    qep, explanation = analyze_query(query)
    print(explanation)
