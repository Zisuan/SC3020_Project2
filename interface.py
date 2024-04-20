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

        # Explanation Display Area
        self.label_explanation = tk.Label(left_pane, text="QEP Cost Analysis:")
        self.label_explanation.pack(fill=tk.X, padx=75, pady=5)
        self.explanation_display = scrolledtext.ScrolledText(left_pane, state='disabled', height=15, wrap='none')
        self.explanation_display.pack(fill=tk.BOTH, expand=True, padx=75, pady=(5, 0))
        self.h_scrollbar = tk.Scrollbar(left_pane, orient='horizontal', command=self.explanation_display.xview)
        self.h_scrollbar.pack(fill='x', padx=75, pady=(0, 20))
        self.explanation_display['xscrollcommand'] = self.h_scrollbar.set

        # Canvas for Graphical QEP Display
        self.canvas = tk.Canvas(right_pane, bg="lightgray")
        # Scrollbars for the canvas
        self.canvas_scrollbar_x = tk.Scrollbar(right_pane, orient='horizontal', command=self.canvas.xview)
        self.canvas_scrollbar_y = tk.Scrollbar(right_pane, orient='vertical', command=self.canvas.yview)

        # Attach the canvas to the scrollbars
        self.canvas.config(xscrollcommand=self.canvas_scrollbar_x.set, yscrollcommand=self.canvas_scrollbar_y.set)

        # Pack the scrollbars
        self.canvas_scrollbar_x.pack(side='bottom', fill='x')
        self.canvas_scrollbar_y.pack(side='right', fill='y')

        # Pack the canvas after the scrollbars
        self.canvas.pack(side='left', fill='both', expand=True)



    def draw_graph(self, qep_data):
      
        self.canvas.delete("all")

        # Function to recursively draw nodes and edges
        def draw_node(node, x, y, level_width, parent_coords=None):

            node_width = 100
            node_height = 50
            padding = 20  # Space between nodes
            vertical_spacing = 100  # Space between levels

            # Calculate the leftmost x position for children
            total_children_width = node_width * len(node.get('children', [])) + padding * (len(node.get('children', [])) - 1)
            start_x = x - total_children_width / 2 + node_width / 2

            # Draw the node rectangle
            x1 = x - node_width / 2
            y1 = y - node_height / 2
            x2 = x + node_width / 2
            y2 = y + node_height / 2
            self.canvas.create_rectangle(x1, y1, x2, y2, fill="lightgray")
            self.canvas.create_text(x, y, text=f"{node['type']}\nCost: {node['cost']}")

            # If this node has a parent, draw an edge from the parent
            if parent_coords:
                self.canvas.create_line(parent_coords[0], parent_coords[1], x, y1, arrow=tk.LAST)


            # Calculate positions for children and draw them
            child_x = start_x
            for child in node.get('children', []):
                next_y = y + node_height / 2 + vertical_spacing
                draw_node(child, child_x, next_y, 400, (x, y2))
                child_x += node_width + padding  # Move to the next child's x position

        # Start drawing from the root node
        root_node = qep_data.get('root')  # This assumes you have a 'root' in your QEP data
        if root_node:
            draw_node(root_node, x=self.canvas.winfo_width() // 2, y=50, level_width=self.canvas.winfo_width())

        self.canvas.update_idletasks()  # Update the canvas to ensure all items are drawn
        self.canvas.config(scrollregion=self.canvas.bbox("all"))

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
