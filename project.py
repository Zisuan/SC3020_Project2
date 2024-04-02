from tkinter import Tk
import interface
import explain

def on_query_submit(query):
    # Passing the query to explain.py for analysis and then display the results.
    qep, explanation = explain.analyze_query(query)
    interface.display_explanation(explanation)


if __name__ == '__main__':
    # Initialize the GUI
    root = Tk()
    app_gui = interface.AppGUI(root, on_query_submit_callback=on_query_submit)
    
    # Run the application
    root.mainloop()