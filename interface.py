import tkinter as tk
from tkinter import messagebox, scrolledtext

class AppGUI:
    def __init__(self, master, on_submit_callback):
        self.master = master
        self.on_submit_callback = on_submit_callback  # Store the callback function

        master.title("QEP Cost Analysis Tool")
        master.geometry('800x600')

        # SQL Query Input label and area
        self.label_input = tk.Label(master, text="Enter your SQL query:")
        self.label_input.pack(fill=tk.X, padx=75, pady=5)  
        self.query_input = scrolledtext.ScrolledText(master, undo=True, height=10)
        self.query_input.pack(fill=tk.BOTH, expand=True, padx=75, pady=5) 

        # Submit Button
        self.submit_button = tk.Button(master, text="Analyze Query", command=self.on_submit)
        self.submit_button.pack(pady=5)

        # Explanation Display Area label and area
        self.label_explanation = tk.Label(master, text="QEP Cost Analysis:")
        self.label_explanation.pack(fill=tk.X, padx=75, pady=5)
        self.explanation_display = scrolledtext.ScrolledText(master, state='disabled', height=15, wrap='none')
        self.explanation_display.pack(fill=tk.BOTH, expand=True, padx=75, pady=(5, 0))
        self.h_scrollbar = tk.Scrollbar(master, orient='horizontal', command=self.explanation_display.xview)
        self.h_scrollbar.pack(fill='x', padx=75, pady=(0, 20))
        self.explanation_display['xscrollcommand'] = self.h_scrollbar.set

    def on_submit(self):
        query = self.query_input.get("1.0", tk.END).strip()
        if query:
            try:
                self.on_submit_callback(query)  # Call the callback function with the query
            except Exception as e:#error handling
                self.display_error(str(e))
        else:
            self.display_error("Please enter a SQL query.") #error handling

    def display_error(self, message):
        messagebox.showerror("Error", message)

    def display_explanation(self, explanation):
        self.explanation_display.config(state='normal')
        self.explanation_display.delete("1.0", tk.END)
        self.explanation_display.insert(tk.END, explanation)
        self.explanation_display.config(state='disabled')
