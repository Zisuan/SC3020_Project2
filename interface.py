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
        self.label_input.pack(pady=5)
        self.query_input = scrolledtext.ScrolledText(master, undo=True, height=10)
        self.query_input.pack(pady=5)

        # Submit Button
        self.submit_button = tk.Button(master, text="Analyze Query", command=self.on_submit)
        self.submit_button.pack(pady=5)

        # Explanation Display Area label and area
        self.label_explanation = tk.Label(master, text="QEP Cost Analysis:")
        self.label_explanation.pack(pady=5)
        self.explanation_display = scrolledtext.ScrolledText(master, state='disabled', height=15)
        self.explanation_display.pack(pady=5)

    def on_submit(self):
        query = self.query_input.get("1.0", tk.END).strip()
        if query:
            self.on_submit_callback(query)  # Call the callback function with the query
        else:
            messagebox.showerror("Error", "Please enter a SQL query.")

    def display_explanation(self, explanation):
        self.explanation_display.config(state='normal')
        self.explanation_display.delete("1.0", tk.END)
        self.explanation_display.insert(tk.END, explanation)
        self.explanation_display.config(state='disabled')
