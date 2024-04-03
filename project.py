import tkinter as tk
from interface import AppGUI
import explain

def on_query_submit(query):
    # Passing the query to explain.py for analysis and then display the results.
    qep, explanation = explain.analyze_query(query)
    app_gui.display_explanation(explanation)


if __name__ == '__main__':
    # Initialize the GUI
    root = tk.Tk()
    app_gui = AppGUI(root, on_submit_callback=on_query_submit)
    
    # Run the application
    root.mainloop()