import tkinter as tk
from tkinter import ttk
from tkinter import messagebox, scrolledtext, Canvas



class AppGUI:
    def __init__(self, master, on_submit_callback):
        self.master = master
        self.on_submit_callback = on_submit_callback

        master.title("QEP Cost Analysis Tool")
        master.geometry('1200x600')  # Adjust the size as necessary

        # Create a PanedWindow widget
        paned_window = ttk.PanedWindow(master, orient=tk.HORIZONTAL)
        paned_window.pack(fill=tk.BOTH, expand=True)

        # Left pane will contain the text areas
        left_pane = ttk.Frame(paned_window)
        paned_window.add(left_pane, weight=1)

        # Right pane will contain the canvas
        right_pane = ttk.Frame(paned_window)
        paned_window.add(right_pane, weight=2)  # Give it more weight so it's wider

        # SQL Query Input label and area
        self.label_input = tk.Label(left_pane, text="Enter your SQL query:")
        self.label_input.pack(fill=tk.X, padx=75, pady=5)
        self.query_input = scrolledtext.ScrolledText(left_pane, undo=True, height=10)
        self.query_input.pack(fill=tk.BOTH, expand=True, padx=75, pady=5)

        # Submit Button
        self.submit_button = tk.Button(left_pane, text="Analyze Query", command=self.on_submit)
        self.submit_button.pack(pady=5)

        # Explanation Display Area label and area
        self.label_explanation = tk.Label(left_pane, text="QEP Cost Analysis:")
        self.label_explanation.pack(fill=tk.X, padx=75, pady=5)
        self.explanation_display = scrolledtext.ScrolledText(left_pane, state='disabled', height=15, wrap='none')
        self.explanation_display.pack(fill=tk.BOTH, expand=True, padx=75, pady=(5, 0))

        # Canvas for Graphical QEP Display in the right pane
        self.canvas = tk.Canvas(right_pane, bg="white")
        self.canvas.pack(fill=tk.BOTH, expand=True)

        # Add horizontal scrollbar for explanation display
        self.h_scrollbar = tk.Scrollbar(left_pane, orient='horizontal', command=self.explanation_display.xview)
        self.h_scrollbar.pack(fill='x', padx=75, pady=(0, 20))
        self.explanation_display['xscrollcommand'] = self.h_scrollbar.set

    def draw_graph(self, qep_data):
      
        self.canvas.delete("all")

        # Function to recursively draw nodes and edges
        def draw_node(node, x, y, level_width, parent_coords=None):
            width = 100  # Width of the node
            height = 50  # Height of the node
            x1 = x - width // 2
            y1 = y - height // 2
            x2 = x + width // 2
            y2 = y + height // 2

            # Draw the node rectangle
            self.canvas.create_rectangle(x1, y1, x2, y2, fill="lightgray")

            # Display the node type and cost within the rectangle
            self.canvas.create_text(x, y, text=f"{node['type']}\nCost: {node['cost']}")

            # If this node has a parent, draw an edge from the parent
            if parent_coords:
                self.canvas.create_line(parent_coords, (x, y1), arrow=tk.LAST)

            # Calculate positions for children and draw them
            child_count = len(node.get('children', []))
            child_x = x - level_width // 2 + level_width // (child_count + 1)
            child_y = y + 100  # Vertical spacing between levels
            for child in node.get('children', []):
                draw_node(child, child_x, child_y, level_width // child_count, (x, y2))
                child_x += level_width // (child_count + 1)

        # Start drawing from the root node
        root_node = qep_data.get('root')  # This assumes you have a 'root' in your QEP data
        if root_node:
            draw_node(root_node, x=self.canvas.winfo_width() // 2, y=50, level_width=self.canvas.winfo_width())

    def on_submit(self):
        query = self.query_input.get("1.0", tk.END).strip()
        if query:
            self.on_submit_callback(query)  # Call the callback, which updates the GUI directly
        else:
            self.display_error("Please enter a SQL query.")

    def display_error(self, message):
        messagebox.showerror("Error", message)

    def display_explanation(self, explanation):
        self.explanation_display.config(state='normal')
        self.explanation_display.delete("1.0", tk.END)
        self.explanation_display.insert(tk.END, explanation)
        self.explanation_display.config(state='disabled')
