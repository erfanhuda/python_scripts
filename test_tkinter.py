import os
import tkinter as tk
from tkinter import BOTH, PhotoImage, ttk, filedialog
from abc import ABC
from functools import partial
from tkinter import messagebox
import psycopg2
from traitlets import default

def connection_string():
    conn = psycopg2.connect(database="seabank", user='postgres', password='erfan123', host='127.0.0.1', port= '5432')
    cursor = conn.cursor()
    
    cursor.execute("SELECT version();")
    data = cursor.fetchone()
    status = ("Connection established", data)
    conn.close()
    
    return status

class ECL_Menu(ttk.Frame):
    def __init__(self,parent):
        super().__init__(parent)

        self.name = "ECL Amount"
        self.place(x=0, y=0,relwidth=0.3, relheight=1)
        self.create_widgets()

    def create_widgets(self):
        self.tabs = ttk.Notebook(self, name="tabnav", width=self.winfo_x(), height=self.winfo_y())

        # Frames Navigation
        self.frame_zero = ttk.Frame(self.tabs)
        self.frame_one = ttk.Frame(self.tabs)
        self.frame_two = ttk.Frame(self.tabs)
        self.frame_three = ttk.Frame(self.tabs)

        # Content of each Frames
        self.label_zero = ttk.Label(self.frame_zero, text="Dashboard")
        # self.label_one = ttk.Label(self.frame_one, text="ECL Flux")
        self.label_two = ttk.Label(self.frame_two, text="Daily Flowrate")
        self.label_three = ttk.Label(self.frame_three, text="II & IIR")

        # ECL TABLES
        columns = ('pt_date', 'pd_segment', 'lgd_segment', 'ecl_total')
        self.table_ecl = ttk.Treeview(self.frame_one, columns=columns, show='headings')
        self.table_ecl.heading('pt_date', text='pt_date')
        self.table_ecl.heading('pd_segment', text='pd_segment')
        self.table_ecl.heading('lgd_segment', text='lgd_segment')
        self.table_ecl.heading('ecl_total', text='ecl_total')

        self.table_ecl_scrollbar = ttk.Scrollbar(self.table_ecl, orient=tk.HORIZONTAL, command=self.table_ecl.yview)
        self.table_ecl.configure(yscroll=self.table_ecl_scrollbar.set)
        self.table_ecl_scrollbar.pack(side='bottom',fill="x") 
        self.table_ecl.pack(padx=5, pady=5,fill='both', expand=True)

        # self.table_ecl.bind("<<TreeviewSelect>>", )
        
        # Pack of frame content
        self.label_zero.pack(padx=5, pady=5)
        # self.label_one.pack(padx=5, pady=5)
        self.label_two.pack(padx=5, pady=5)
        self.label_three.pack(padx=5, pady=5)

        # Pack of frame
        self.frame_zero.pack(padx=5, pady=5)
        self.frame_one.pack(padx=5, pady=5)
        self.frame_two.pack(padx=5, pady=5)
        self.frame_three.pack(padx=5, pady=5)

        # Add the frame to the tabs
        self.tabs.add(self.frame_zero,text="Dashboard")
        self.tabs.add(self.frame_one,text="Flux")
        self.tabs.add(self.frame_two,text="Flowrate")
        self.tabs.add(self.frame_three,text="Interest Income Rate")
        
        # Pack tabs navigation to parents
        self.tabs.pack(padx=5,pady=5,fill='both',expand=True)
class OJK_Menu(ttk.Frame):
    def __init__(self,parent):
        super().__init__(parent)

        self.name = "OJK Buku 3 NPL"
        self.place(x=0, y=0,relwidth=0.3, relheight=1)
        self.create_widgets()

    def create_widgets(self):
        self.url_entry = ttk.Entry(self, show="http://localhost:1542")
        self.url_entry.pack(padx=5, pady=5, side="left")

class FL_Menu(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        
        self.name = "Forward Looking"
        self.place(x=0, y=0,relwidth=0.3, relheight=1)
        self.create_widgets()

    def create_widgets(self):
        # Tab Navigation
        self.tabs = ttk.Notebook(self, name="tabnav", width=self.winfo_x(), height=self.winfo_y())

        # Frames Navigation
        self.frame_zero = ttk.Frame(self.tabs)
        self.frame_one = ttk.Frame(self.tabs)
        self.frame_two = ttk.Frame(self.tabs)
        self.frame_three = ttk.Frame(self.tabs)

        # Content of each Frames
        self.label_two = ttk.Label(self.frame_two, text="This is SFA")
        self.label_three = ttk.Label(self.frame_three, text="This is Frame MFA")

        # Add notepad in frame zero
        self.input_frame = ttk.Frame(self.frame_zero)
        self.entry = tk.Label(self.input_frame)
        self.__open_json_file = ttk.Button(self.input_frame, text="Open JSON File", command=self.__open_json_file)

        self.entry.pack(padx=5,side='left',fill='x', expand=True)
        self.__open_json_file.pack(padx=5,side='left', fill='x', expand=False)
        self.input_frame.pack(side='top',fill='x',expand=False)

        self.textpad = tk.Text(self.frame_zero)
        self.textpad.pack(padx=5,pady=5, fill='both', expand=True)

        self.__save_json_file = ttk.Button(self.frame_zero, text="Save", command=self.__save_json_file)
        self._run_file = ttk.Button(self.frame_zero, text="Run")
        self.__save_json_file.pack(padx=5,pady=5,side='left', fill='x', expand=False)
        self._run_file.pack(padx=5,pady=5,side='right', fill='x', expand=False)

        # Add button in frame one
        self.input_frame_one = ttk.Frame(self.frame_one)
        self.entry_one = tk.Label(self.input_frame_one)
        self.__add_button = ttk.Button(self.input_frame_one, text="Add", command=self.__add_var_button)

        self.entry_one.pack(padx=5,side='left',fill='x', expand=True)
        self.__add_button.pack(padx=5,side='left', fill='x', expand=False)
        self.input_frame_one.pack(side='top',fill='x',expand=False)
        # Add treeview in frame one
        columns = ("date", "var0")
        self.variable_tables = ttk.Treeview(self.frame_one, columns=columns,show="headings")
        self.variable_tables.heading("date", text="Date")
        self.variable_tables.heading("var0", text="Variable0")

        variables = []
        for var in variables:
            self.variable_tables.insert('',tk.END,values=var)

        # self.variable_tables.grid(row=0, column=0, sticky="nsew")
        
        table_scroll = ttk.Scrollbar(self.variable_tables, orient=tk.HORIZONTAL, command=self.variable_tables.yview)
        self.variable_tables.configure(yscroll=table_scroll.set)
        table_scroll.pack(side='bottom',fill='both',expand=False)
        
        # Pack of frame content
        self.variable_tables.pack(padx=5,pady=5,fill='both',expand=True)
        self.label_two.pack(padx=5, pady=5)
        self.label_three.pack(padx=5, pady=5)

        # Pack of frame
        self.frame_one.pack(padx=5, pady=5)
        self.frame_zero.pack(padx=5, pady=5)
        self.frame_two.pack(padx=5, pady=5)
        self.frame_three.pack(padx=5, pady=5)

        # Add the frame to the tabs
        self.tabs.add(self.frame_one,text="Variables")
        self.tabs.add(self.frame_zero,text="Configuration")
        self.tabs.add(self.frame_two,text="SFA")
        self.tabs.add(self.frame_three,text="MFA")
        
        # Pack tabs navigation to parents
        self.tabs.pack(padx=5,pady=5,fill='both',expand=True)

    def __open_json_file(self):
        self._file = filedialog.askopenfilename(defaultextension=".json", filetypes=[("All Files", "*.*"), ("JSON Files", ".json")])
        if self._file == "":  
            # If there is no file to open  
            self._file = None  
        else:  
            # For trying to open the file set the window title  
            self.entry.configure(text=os.path.abspath(self._file))
            self.textpad.delete(1.0, tk.END)  
  
            file = open(self._file, "r")  
  
            self.textpad.insert(1.0, file.read())  
  
            file.close()  

    def __save_json_file(self):
        
        if self._file == None:  
            # For Saving as new file  
            self._file = filedialog.asksaveasfile(mode="w",confirmoverwrite=True,defaultextension=".json")  
  
            if self._file == "":  
                self._file = os.path.basename(self._file)
            else:  
                  
                # For trying to  save the file  
                file = open(self._file,"w")  
                file.write(self.textpad.get(1.0, tk.END))  

                messagebox.showinfo("Information","File successfully saved")

                
                file.close()  
                  
                # For changing the label title  
                self.entry.configure(text=os.path.abspath(self._file))
              
        else:  
            file = open(self._file,"w")  
            file.write(self.textpad.get(1.0, tk.END))  
            messagebox.showinfo("Information","File successfully saved")
            file.close()  

    def __add_var_button(self):
        self._file = filedialog.askopenfilename(defaultextension=".csv", filetypes=[("CSV Files", ".csv")])
        if self._file == "":  
            # If there is no file to open  
            self._file = None  
        else:  
            # For trying to open the file set the window title  
            self.entry.configure(text=os.path.abspath(self._file))
            self.textpad.delete(1.0, tk.END)  
  
            file = open(self._file, "r")  
  
            self.textpad.insert(1.0, file.read())  
  
            file.close()  

class SettingMenu(tk.Toplevel):
    def __init__(self,parent):
        super().__init__(parent)
        
        self.geometry("500x500")
        self.resizeable(False,False)

        self.create_widgets()

    def create_widgets(self):
        # Tab Navigation
        self.tabs = ttk.Notebook(self, name="tabnav", width=self.winfo_x(), height=self.winfo_y())

        # Frames Navigation
        self.frame_zero = ttk.Frame(self.tabs)
        self.frame_one = ttk.Frame(self.tabs)
        self.frame_two = ttk.Frame(self.tabs)
        self.frame_three = ttk.Frame(self.tabs)

        # Content of each Frames
        self.label_zero = ttk.Label(self.frame_zero, text="This is Files")
        self.entry_zero = ttk.Entry(self.frame_zero)
        self.label_one = ttk.Label(self.frame_one, text="This is Variables")
        self.label_two = ttk.Label(self.frame_two, text="This is SFA")
        self.label_three = ttk.Label(self.frame_three, text="This is Frame MFA")
        
        # Pack of frame content
        self.label_zero.pack(padx=5, pady=5)
        self.entry_zero.pack(padx=5, pady=5)
        self.label_one.pack(padx=5, pady=5)
        self.label_two.pack(padx=5, pady=5)
        self.label_three.pack(padx=5, pady=5)

        # Pack of frame
        self.frame_zero.pack(padx=5, pady=5)
        self.frame_one.pack(padx=5, pady=5)
        self.frame_two.pack(padx=5, pady=5)
        self.frame_three.pack(padx=5, pady=5)

        # Add the frame to the tabs
        self.tabs.add(self.frame_zero,text="Files")
        self.tabs.add(self.frame_one,text="Variables")
        self.tabs.add(self.frame_two,text="SFA")
        self.tabs.add(self.frame_three,text="MFA")
        
        # Pack tabs navigation to parents
        self.tabs.pack(padx=5,pady=5,fill='both',expand=True)

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.geometry("600x400")

        # Notebook version
        self.tabs = ttk.Notebook(self, name="tabnav", width=self.winfo_x(), height=self.winfo_y())
        self.tabs.bind("<<NotebookTabChanged>>", self.handle_add_tab)
        self.tabs.pack(padx=10,pady=10,fill='both',expand=True)

        self.empty_frame = ttk.Frame()
        self.tabs.add(self.empty_frame,text="+")


        # Status bar
        self.bottom_bar = ttk.Frame()
        self.bar_label = ttk.Label(self.bottom_bar,text=str(connection_string()))
        self.bar_label.pack(side="left", padx=5,pady=5)
        self.bottom_bar.pack(side="bottom", fill="x")

        self.create_menu()
        
    def create_menu(self):
        # Add menubars
        self.menu = tk.Menu(self)
        self.config(menu=self.menu)

        modeling_menu = tk.Menu(self.menu)
        modeling_menu.add_command(label="Forward Looking", command=partial(self.open_menu, FL_Menu))
        modeling_menu.add_command(label="OJK NPL Buku", command=partial(self.open_menu, OJK_Menu))

        ecl_menu = tk.Menu(self.menu)
        ecl_menu.add_command(label="ECL", command=partial(self.open_menu, ECL_Menu))

        setting_menu = tk.Menu(self.menu)
        setting_menu.add_command(label="Global Settings", command=partial(self.open_pop_up, SettingMenu))

        self.menu.add_cascade(label="ECL Model", menu=modeling_menu)
        self.menu.add_cascade(label="ECL Data", menu=ecl_menu)
        self.menu.add_cascade(label="Setting", menu=setting_menu)

    def handle_add_tab(self, event):
        if self.tabs.select() == self.tabs.tabs()[-1]:
            index = len(self.tabs.tabs())-1
            frame = ttk.Frame(self.tabs)
            self.tabs.insert(index, frame, text="<Untitled>")
            self.tabs.select(index)

    def open_menu(self, obj=None):
        if obj is not None:
            app = obj(self.tabs)
            
        if self.tabs.tab(self.tabs.select(), "text") == "<Untitled>":
            index = self.tabs.index(self.tabs.select())
            app.pack(padx=5, pady=5)
            self.tabs.insert(index, app, text=app.name)
            index += 1
            self.tabs.select(index - 1)
            self.tabs.forget(index)
        else: 
            index = self.tabs.index(self.tabs.select())
            app.pack(padx=5, pady=5)
            self.tabs.insert(index, app, text=app.name)
            index += 1
            self.tabs.select(index - 1)

    def open_pop_up(self, obj=None):
        if obj is not None:
            app = obj(self)

        obj(self)
        obj.resiable(False,False)


if __name__ == "__main__":
    connection_string()
    app = App()
    app.iconbitmap("logo/logo-sea.ico")
    app.title("Seabank - Finance Project")
    app.mainloop()