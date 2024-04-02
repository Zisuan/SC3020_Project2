import psycopg2
import psycopg2.extras
import json

def connect_to_db():
    # Create a connection to the PostgreSQL database.
    try:
        conn = psycopg2.connect(
            dbname='TPC-H',
            user='localhost',
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
    
    # Sample placeholder logic
    explanation = "Query Plan Explanation:\n"
    for plan in qep[0]['Plan']:
        explanation += explain_node(plan)
    return explanation 


def explain_node(node, depth=0):
    # Recursively explain a single node in the QEP.
    # Placeholder for explanation logic for a single node.
    # You might want to detail the type of scan, estimated rows, cost, etc.
    explanation = "\t"*depth + f"Node Type: {node['Node Type']}, Cost: {node['Total Cost']}\n"
    if 'Plans' in node:
        for child in node['Plans']:
            explanation += explain_node(child, depth + 1)
    return explanation

if __name__ == '__main__':
    # Example usage for testing
    query = "SELECT * FROM your_table WHERE your_condition;"
    qep, explanation = analyze_query(query)
    print(explanation)
