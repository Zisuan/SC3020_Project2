import psycopg2
import psycopg2.extras
import json
from psycopg2.sql import SQL, Identifier

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
                cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}")  # Include ANALYZE and BUFFERS for detailed runtime stats
                explain_result = cur.fetchone()
                conn.close()
                return explain_result[0]
        except Exception as e:
            print(f"An error occurred while executing EXPLAIN: {e}")
            return None
    else:
        return None

def analyze_query(query, cursor):
    # Analyze the given SQL query, returning a QEP and its cost explanation.
    # No changes to execute_explain(), since it creates its own connection and cursor.
    qep = execute_explain(query)
    if qep is not None:
        explained_nodes = set()
        explanation = parse_and_explain(qep, cursor, explained_nodes)  # Pass explained_nodes here
        return qep, explanation
    else:
        print("Failed to fetch the QEP from the database.")
        return None, ""

def parse_and_explain(qep_json, cursor, explained_nodes):
    # Parse the QEP and generate an explanation of cost estimations.
    explanation = "Query Plan Explanation:\n"
    for qep_part in qep_json:
        plan = qep_part['Plan']
        explanation += explain_node(plan, cursor, 0, explained_nodes)  
    return explanation


def explain_node(node, cursor, depth=0, explained_nodes=None):
    # Recursively explain a single node in the QEP.
    
    if explained_nodes is None:
        explained_nodes = set()

    # Create a unique identifier for the node to check if it's been explained
    node_identifier = (node.get('Node Type'), node.get('Plan Rows'), node.get('Total Cost'))

    if node_identifier in explained_nodes:
        # Skip this node to prevent duplicate explanations
        return ''
    explained_nodes.add(node_identifier)

    # Start building the explanation string with node type and basic cost details
    node_type = node.get('Node Type', 'Unknown')
    startup_cost = node.get('Startup Cost', 'Unknown')
    total_cost = node.get('Total Cost', 'Unknown')
    rows = node.get('Plan Rows', 'Unknown')
    width = node.get('Plan Width', 'Unknown')

    explanation = "------------------\n"
    explanation += f"Node Type: {node_type} (Cost: {startup_cost}..{total_cost} rows={rows} width={width})\n"

    # Detailed explanation for the node type
    explanations = {
        "Hash Join": (
            "The Hash Join performs an equi-join between two tables by hashing one, typically the smaller one, "
            "which allows for efficient lookups during the join. It is most effective when there is a large "
            "disparity in the size of the tables or when the join condition is based on equality."
        ),
        "Nested Loop": (
            "The Nested Loop Join is used for joining two tables by scanning the inner table for each row of the outer table. "
            "It's typically used when one of the tables is small enough to fit in memory, or when an index is used on the inner table."
        ),
        "Seq Scan": (
            "The Sequential Scan reads through an entire table sequentially. It is used when it is more efficient "
            "to scan the whole table than to use an index, such as when a large fraction of the rows need to be retrieved."
        ),
        "Index Scan": (
            "The Index Scan uses an index to retrieve rows in a specific order. It is effective when the query "
            "requires only a small subset of rows. This operation can be much faster than a sequential scan if "
            "the index is well-tuned to the query's conditions."
        )
        # Continue adding explanations for other node types...
    }
    
    # Append the detailed explanation if it exists
    if node_type in explanations:
        explanation += f"Description: {explanations[node_type]}\n"

    # Add additional details like Hash Cond, Filter, and Index Cond if present
    if 'Hash Cond' in node:
        explanation += f"Hash Cond: {node['Hash Cond']}\n"
    if 'Filter' in node:
        explanation += f"Filter: {node['Filter']}\n"
    if 'Index Cond' in node:
        explanation += f"Index Cond: {node['Index Cond']}\n"
    # Add other additional details
        

    # Specific explanations for different node types
    if node.get('Node Type') == "Seq Scan":
        explanation += explain_sequential_scan(node, depth + 1) + "\n"
    elif node.get('Node Type') == "Index Scan":
        explanation += explain_index_scan(node, depth + 1) + "\n"
    elif node.get('Node Type') == "Hash Join":
        explanation += explain_hash_join(node, depth + 1)
    elif node.get('Node Type') == "Aggregate":
        explanation += explain_aggregate(node, depth + 1)

    explanation += add_theoretical_costs(node, cursor)

    # Recursively explain child nodes if they exist
    if 'Plans' in node:
        for child in node['Plans']:
            child_explanation = explain_node(child, cursor, depth + 1, explained_nodes)
            explanation += child_explanation


    return explanation

# Explanation of operations
def explain_sequential_scan(node, depth):
    # Assuming seq_page_cost is a predefined constant for your database
    seq_page_cost = 1.0  # default PostgreSQL configuration
    #explanation = "\t" * (depth + 1)
    table_name = node.get('Relation Name', 'Unknown')
    rows = node.get('Plan Rows', 0)
    # Assuming a simplification where cost is directly proportional to the number of rows
    seq_scan_cost = rows * seq_page_cost
    explanation = f"Sequential Scan on {table_name} estimated to cost {seq_scan_cost}, " \
                   f"based on estimated rows of {rows} and seq_page_cost of {seq_page_cost}.\n"
    return explanation

def explain_index_scan(node, depth):
    index_name = node.get('Index Name', 'Unknown')
    rows = node.get('Plan Rows', 0)
    total_cost = node.get('Total Cost', 'Unknown')
    #explanation = "\t" * depth
    explanation = f"Index Scan using {index_name} estimated to cost {total_cost}, " \
                   f"with estimated rows of {rows}.\n" \
                   f"This cost is derived from the cost to traverse the index and fetch the rows from the table.\n"
    return explanation

def explain_hash_join(node, depth):
    total_cost = node.get('Total Cost', 'Unknown')
    rows = node.get('Plan Rows', 0)
    #explanation = "\t" * depth
    explanation = f"Hash Join estimated to cost {total_cost}, " \
                   f"with estimated rows of {rows}.\n" \
                   f"This cost includes the cost of building a hash table on the join key and then probing the hash table.\n"
    return explanation

def explain_aggregate(node, depth):
    total_cost = node.get('Total Cost', 'Unknown')
    rows = node.get('Plan Rows', 0)
    #explanation = "\t" * depth
    group_key = ", ".join(node.get('Group Key', []))
    explanation = f"Aggregate (Group Key: {group_key}) estimated to cost {total_cost}, " \
                   f"with estimated rows of {rows}.\n" \
                   f"This cost reflects the computation and grouping of rows based on the group key.\n"
    return explanation

def get_relation_stats(cursor, relation_name):
    cursor.execute(SQL("SELECT reltuples, relpages FROM pg_class WHERE relname = %s;"), (relation_name,))
    result = cursor.fetchone()
    if result is None:
        raise ValueError(f"Stats not found for relation: {relation_name}")
    return result

def get_attribute_stats(cursor, relation_name, attribute_name):
    # Fetch the number of distinct values for a given attribute in a relation
    cursor.execute(SQL("SELECT n_distinct FROM pg_stats WHERE tablename = %s AND attname = %s;"), (relation_name, attribute_name))
    return cursor.fetchone()[0]

# Function to calculate and add theoretical costs to the explanation
def add_theoretical_costs(node, cursor):
    explanation = ""
    node_type = node.get('Node Type')

    if 'Relation Name' in node:
        relation_name = node['Relation Name']
        tuples, pages = get_relation_stats(cursor, relation_name)
    else:
        tuples = pages = None  # Defaults in case we can't get specific stats
        
    # Adjust formula based on node type
    if node_type in ["Hash Join", "Nested Loop", "Merge Join"]:
        # For join nodes, you might want to handle them differently since they operate on the results of child nodes
        # Example for handling a hash join's theoretical cost:
        if node_type == "Hash Join":
            # This is a simplification; adjust according to your formulas and possibly consider child node stats
            if tuples and pages:
                theoretical_cost = 3 * (pages + tuples)  # Placeholder formula
                explanation = f"Theoretical Cost (Hash Join): {theoretical_cost}\n"
        # Add handling for other joins and operations as necessary
    elif tuples and pages:
        # Handle single-relation operations
        if node_type == "Seq Scan":
            # Adjust with the actual formula for sequential scans
            theoretical_cost = tuples * pages  # Placeholder formula
            explanation = f"Theoretical Cost (Seq Scan): {theoretical_cost}\n"
        # Add more conditions for other operations

    return explanation


if __name__ == '__main__':
    # Example usage for testing
    query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey AND O.o_orderstatus='F' AND O.o_shippriority = 0 AND C.c_custkey < 500;"
    conn = connect_to_db()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        qep_json, explanation = analyze_query(query, cursor)
        if qep_json:
            main_plan = qep_json[0]['Plan']  # Assuming qep_json is not None and is a list
            explanation += explain_node(main_plan, cursor, 0, explained_nodes=set())  # Initialize explained_nodes as an empty set
        print(explanation)
        conn.close()
