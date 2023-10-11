import tkinter as tk
from tkinter import BOTH, PhotoImage, ttk, filedialog
from abc import ABC


class FloatingWindow(tk.Toplevel):
    def __init__(self, *args, **kwargs):
        tk.Toplevel.__init__(self, *args, **kwargs)
        self.overrideredirect(True)

        self.label = tk.Label(self, text="Click on the grip to move")
        self.grip = tk.Label(self, bitmap="gray25")
        self.grip.pack(side="left", fill="y")
        self.label.pack(side="right", fill="both", expand=True)

        self.grip.bind("<ButtonPress-1>", self.start_move)
        self.grip.bind("<ButtonRelease-1>", self.stop_move)
        self.grip.bind("<B1-Motion>", self.do_move)

    def start_move(self, event):
        self.x = event.x
        self.y = event.y

    def stop_move(self, event):
        self.x = None
        self.y = None

    def do_move(self, event):
        deltax = event.x - self.x
        deltay = event.y - self.y
        x = self.winfo_x() + deltax
        y = self.winfo_y() + deltay
        self.geometry(f"+{x}+{y}")

class MainWindow(tk.Tk):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        self.geometry("500x500")

class App(tk.Tk):
    def __init__(self, *args, **kwargs):
        tk.Tk.__init__(self)
        self.iconbitmap("logo/logo-sea.ico")
        self.title("Seabank Modeling")
        self.geometry("500x500")
        self.x = self.winfo_x()
        self.y = self.winfo_y()

        print(self.y)

        # Input file json
        # self.file_config = filedialog.askopenfile()
        self.file_frame = ttk.Frame(self)
        self.filename_dialog = ttk.Entry(self.file_frame, text="filename", state="disabled")
        self.dial_button = ttk.Button(self.file_frame, text="Open")

        self.file_frame.pack(padx=5,pady=5)
        self.filename_dialog.grid(row=0, column=0)
        self.dial_button.grid(row=0, column=1)
        
        # Tab Navigation
        self.tabs = ttk.Notebook(self, name="tabnav", width=self.winfo_x(), height=self.winfo_y())

        self.frame_one = ttk.Frame(self.tabs)
        self.frame_two = ttk.Frame(self.tabs)
        self.frame_three = ttk.Frame(self.tabs)
        self.label_one = ttk.Label(self.frame_one, text="This is Variables")
        self.label_two = ttk.Label(self.frame_two, text="This is SFA")
        self.label_three = ttk.Label(self.frame_three, text="This is Frame MFA")

        self.label_one.pack(padx=5, pady=5)
        self.label_two.pack(padx=5, pady=5)
        self.label_three.pack(padx=5, pady=5)

        self.frame_one.pack(padx=5, pady=5)
        self.frame_two.pack(padx=5, pady=5)
        self.frame_three.pack(padx=5, pady=5)

        self.tabs.add(self.frame_one,text="Variables")
        self.tabs.add(self.frame_two,text="SFA")
        self.tabs.add(self.frame_three,text="MFA")
        
        self.tabs.pack(padx=5,pady=5)

        # The treeview
        # col = ("Variables", "p_value", "MAPE", "MSE")
        # self.treeview = ttk.Treeview(self.tabs[0], columns=col, height=10, name="variables")
        # self.treeview.pack(padx=5,pady=5)

if __name__ == "__main__":
    app = App()
    app.mainloop()