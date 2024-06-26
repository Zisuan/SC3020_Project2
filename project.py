import tkinter as tk
from interface import AppGUI
from explain import DBConnection, analyze_query

def on_query_submit(query):
    try:
        with DBConnection('TPC-H', 'postgres', 'password', 'localhost', "5432") as conn:
            qep, explanation, graph_data = analyze_query(query, conn)
            app_gui.display_explanation(explanation)
            if graph_data:  # Check if graph_data is not None or empty
                app_gui.draw_graph(graph_data)
    except Exception as e: #error handling
        app_gui.display_error(str(e))

if __name__ == '__main__':
    # Initialize GUI
    root = tk.Tk()
    app_gui = AppGUI(root, on_submit_callback=on_query_submit)
    
    # Run the app
    root.mainloop()
