import csv
from io import StringIO
from multiprocessing import Process
import os
import pickle
import threading
import tkinter as tk
from tkinter import BOTH, PhotoImage, ttk, filedialog
from abc import ABC
from functools import partial
from tkinter import messagebox
import itertools

import pandas as pd
import f_statistic as f
# import psycopg2

def build_connection(*args, **kwargs):
    # conn = psycopg2.connect(database=kwargs['database'], user=kwargs['user'], password=kwargs['password'], host=kwargs['host'], port= kwargs['port'])
    # cursor = conn.cursor()
    
    # cursor.execute("SELECT version();")
    # data = cursor.fetchone()
    status = ("Connection established", "Yea")
    # conn.close()
    
    return status

class ConfigurationTable(ttk.Treeview):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.bind("<Double-1>", self.on_double_click)

    def on_double_click(self, e):
        region_clicked = self.identify_region(e.x,e.y)
        
        if region_clicked not in ("tree", "cell"):
            return
        
        # Item yg diklik dua kali
        # Contoh, "#0" adalah kolom pertama, berikutnya "#1", dst.
        column = self.identify_column(e.x)
        column_index = int(column[1:]) - 1

        # Row Item yg diklik dua kali
        selected_iid = self.focus()
        selected_values = self.item(selected_iid)

        if column == "#0":
            selected_values = selected_values.get("text")
        else:
            selected_values = selected_values.get("values")[column_index]

        column_box = self.bbox(selected_iid, column)
        edit_box = ttk.Entry(self, width=column_box[2])

        # Koordinat dari edit box based on col index & iid selain col 0
        if column == "#0":
            return
        else:
            edit_box.column = column_index
            edit_box.row = selected_iid

            edit_box.insert(0, selected_values)
            edit_box.select_range(0, tk.END)
            edit_box.focus()
            edit_box.bind("<FocusOut>", self.on_focus_out)
            edit_box.bind("<Return>", self.on_return)

            edit_box.place(x=column_box[0], y=column_box[1], width=column_box[2], height=column_box[3])
    
    def on_focus_out(self, e):
        e.widget.destroy()

    def on_return(self, e):
        new_text = e.widget.get()
        selected_iid = e.widget.row
        col_index = e.widget.column

        if col_index == -1:
            return
        else:
            current_values = self.item(selected_iid).get("values")
            current_values[col_index] = new_text
            self.item(selected_iid, values=current_values)

        e.widget.destroy()


class VariablesTable(ttk.Treeview):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)

    def lower_first(self, iterator):
        return itertools.chain([next(iterator).lower()], iterator)

    def on_append_files(self, files):
        csvreader = csv.DictReader(self.lower_first(files))
        # csvreader = pd.read_csv(files, delimiter=",")
        # csvreader['Date'] = pd.to_datetime(csvreader['Date'])
        items = []
        print(*csvreader)

        # for row in csvreader:
        #     if "Date" in row.keys():
        #         items.append(row)
        #     else:
        #         print("Check the date fields.")
        #         print(row)

        # headers = [header for header in items[0].keys()]

        # if "date" in headers:
        #     self.heading("#0", text="date")
            
        #     headers.remove('date')
        #     self.configure(columns=headers)
        #     for item in headers:
        #         self.heading(item, text=item)

        # for item in items:
        #     item_values = list(item.values())

        #     date = item_values[0]
        #     values = item_values[1:]

        #     self.insert("", index=tk.END, text=date, values=values)

class ContactsTable(ttk.Treeview):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.bind("<Double-1>", self.on_double_click)

    def on_double_click(self, e):
        region_clicked = self.identify_region(e.x,e.y)
        
        if region_clicked not in ("tree", "cell"):
            return
        
        # Item yg diklik dua kali
        # Contoh, "#0" adalah kolom pertama, berikutnya "#1", dst.
        column = self.identify_column(e.x)
        column_index = int(column[1:]) - 1

        # Row Item yg diklik dua kali
        selected_iid = self.focus()
        selected_values = self.item(selected_iid)

        if column == "#0":
            selected_values = selected_values.get("text")
        else:
            selected_values = selected_values.get("values")[column_index]

class ContactsFrame(ttk.Frame):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.name = "Contacts"
        self.place(x=0,y=0,relwidth=0.3,relheight=1)
        self.create_widgets()

    def create_widgets(self):
        col_names = ("name", "phone", "email", "address")
        self.table = ttk.Treeview(self,columns=col_names,selectmode='extended')
        self.table.heading("#0",text="Type")
        self.table.heading("name",text="Name")
        self.table.heading("phone",text="Phone")
        self.table.heading("email",text="Email")
        self.table.heading("address",text="Address")
        company_row = self.table.insert("", text="Company", index=tk.END)
        self.table.insert(company_row, values=("Box Kayu", "085799663331", "box_kayu@gmail.com", "Jakarta, Indonesia"), index=tk.END)
        persons_row = self.table.insert("", text="Person", index=tk.END)
        self.table.pack(fill=tk.BOTH,expand=True)

        xscroll = tk.Scrollbar(self.table, orient=tk.HORIZONTAL,command=self.table.xview)
        xscroll.pack(padx=5,pady=5,side='bottom',fill='both')
        self.table.configure(xscrollcommand=xscroll.set)
        self.table.bind("<MouseWheel>", lambda e : self.table.yview_scroll(int(-1*(e.delta)), "units"))
        self.table.bind("<Control-MouseWheel>", lambda e : self.table.xview_scroll(int(-1*(e.delta)), "units"))

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
        self.frame_fourth = ttk.Frame(self.tabs)

        # Content of each Frames
        self.label_two = ttk.Label(self.frame_two, text="This is SFA")
        self.label_three = ttk.Label(self.frame_three, text="This is Frame MFA")

        # Add notepad in frame zero
        self._note_config = ttk.Notebook(self.frame_zero)

        self._note_first = ttk.Frame(self._note_config)
        self._note_second = ttk.Frame(self._note_config)
        self._table_config_manual()
        self._note_first.pack(padx=5,pady=5,expand=True,fill='both')
        self._note_second.pack(padx=5,pady=5,expand=True,fill='both')
        self._note_config.pack(padx=5,pady=5,fill='both',expand=True)
        
        self.input_frame = ttk.Frame(self._note_first)
        self.entry = tk.Label(self.input_frame)
        self.__open_json_file = ttk.Button(self.input_frame, text="Open JSON File", command=self.__open_json_file)

        self.entry.pack(padx=5,side='left',fill='x', expand=True)
        self.__open_json_file.pack(padx=5,side='left', fill='x', expand=False)
        self.input_frame.pack(side='top',fill='x',expand=False)

        self.textpad = tk.Text(self._note_first)
        self.textpad.pack(padx=5,pady=5, fill='both', expand=True)

        self.__save_json_file = ttk.Button(self._note_first, text="Save", command=self.__save_json_file)
        self._run_file = ttk.Button(self._note_first, text="Run", command=self._run_script)
        self.__save_json_file.pack(padx=5,pady=5,side='left', fill='x', expand=False)
        self._run_file.pack(padx=5,pady=5,side='right', fill='x', expand=False)

        self._note_config.add(self._note_second, text="Manual")
        self._note_config.add(self._note_first, text="JSON File")

        # Add button in frame one
        self.input_frame_one = ttk.Frame(self.frame_one)
        self.entry_one = tk.Label(self.input_frame_one)
        self.__add_button = ttk.Button(self.input_frame_one, text="Add", command=self.__add_var_button)

        self.entry_one.pack(padx=5,side='left',fill='x', expand=True)
        self.__add_button.pack(padx=5,side='left', fill='x', expand=False)
        self.input_frame_one.pack(side='top',fill='x',expand=False)

        # Add treeview in frame one
        col_names = ("",)
        self.var_tables = VariablesTable(self.frame_one,columns=col_names,selectmode='extended')
        self.var_tables.heading("#0",text="Date")
        self.var_tables.heading("",text="")
        self.var_tables.pack(fill=tk.BOTH,expand=True)

        yscroll = tk.Scrollbar(self.var_tables, orient=tk.VERTICAL,command=self.var_tables.yview)
        xscroll = tk.Scrollbar(self.var_tables, orient=tk.HORIZONTAL,command=self.var_tables.xview)
        xscroll.pack(padx=5,pady=5,side='bottom',fill='both')
        yscroll.pack(padx=5,pady=5,side='right',fill='both')
        self.var_tables.configure(xscrollcommand=xscroll.set,yscrollcommand=yscroll.set)
        self.var_tables.bind("<Control-MouseWheel>", lambda e : self.var_tables.xview_scroll(int(-1*(e.delta)), "units"))

        
        # Pack of frame content
        self.label_two.pack(padx=5, pady=5)
        self.label_three.pack(padx=5, pady=5)

        # Pack of frame
        self.frame_one.pack(padx=5, pady=5)
        self.frame_zero.pack(padx=5, pady=5)
        self.frame_two.pack(padx=5, pady=5)
        self.frame_three.pack(padx=5, pady=5)
        self.frame_fourth.pack(padx=5, pady=5)

        # Add the frame to the tabs
        self.tabs.add(self.frame_one,text="Variables")
        self.tabs.add(self.frame_zero,text="Configuration")
        self.tabs.add(self.frame_fourth,text="Forecast")
        self.tabs.add(self.frame_two,text="SFA")
        self.tabs.add(self.frame_three,text="MFA")
        
        # Pack tabs navigation to parents
        self.tabs.pack(padx=5,pady=5,fill='both',expand=True)

    def _run_script(self):
        app = f.App()
        app.set_file = self._file
        app.run()

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
  
            file = open(self._file, "r", newline="",encoding="utf-8-sig",errors='replace',)
            self.var_tables.on_append_files(file)
  
            file.close()
    
    def _table_config_manual(self):
        # Add buttons in frame fifth
        self.btn_frame = ttk.Frame(self._note_second)
        self._export_json = ttk.Button(self.btn_frame, text="Export to JSON",command=self._export_to_json)
        self._save = ttk.Button(self.btn_frame, text="Save",command=self._on_save_to_json)
        self._export_json.pack(padx=5,pady=5,side="right")
        self._save.pack(padx=5,pady=5,side="right")
        self.btn_frame.pack(padx=5,pady=5,fill="both")

        # Add treeview in frame fifth
        col_names = ("values",)
        self.table = ConfigurationTable(self._note_second,columns=col_names,selectmode='extended')
        self.table.heading("#0",text="Configurations")
        self.table.heading("values",text="Values")

        variable_config = self.table.insert(parent="", iid='1', index=tk.END,text="Set Variables")
        y_var_config = self.table.insert(parent=variable_config, iid='11',index=tk.END,text="Y Variable", values=("ODR",))
        self.table.insert(parent=y_var_config, index=tk.END, iid='111', text="z-score", values=("",))
        self.table.insert(parent=y_var_config, index=tk.END, iid='112',text="Ln", values=("",))
        x_var_config = self.table.insert(parent=variable_config, iid='2', index=tk.END,text="X Variable", values=("GDP",))
        self.table.insert(parent=x_var_config, index=tk.END, iid='21',text="Moving Average (Month)", values=("",))
        self.table.insert(parent=x_var_config, index=tk.END, iid='22',text="Exponential (Month)", values=("",))
        self.table.insert(parent=x_var_config, index=tk.END, iid='23',text="Lag (Month)", values=("",))
        self.table.insert(parent=x_var_config, index=tk.END, iid='24',text="Lead (Month)", values=("",))
        self.table.insert(parent=x_var_config, index=tk.END, iid='25',text="Delta (Month)", values=("",))
        self.table.insert(parent=x_var_config, index=tk.END, iid='26',text="Variance (Month)", values=("",))
        self.table.insert(parent=x_var_config, index=tk.END, iid='27',text="Growth (Month)", values=("",))

        test_sample_config = self.table.insert(parent="", iid='3',index=tk.END,text="Set Samples")
        self.table.insert(parent=test_sample_config,iid='31', index=tk.END,text="Training Date", values=("",))
        self.table.insert(parent=test_sample_config,iid='32', index=tk.END,text="Testing Date", values=("",))
        
        forecast_config = self.table.insert(parent="",iid='4', index=tk.END,text="Forecast")
        self.table.insert(parent=forecast_config,iid='41', index=tk.END,text="Training Date", values=("",))
        self.table.insert(parent=forecast_config,iid='42', index=tk.END,text="Testing Date", values=("",))

        self.table.pack(fill=tk.BOTH,expand=True)


        xscroll = tk.Scrollbar(self.table, orient=tk.HORIZONTAL,command=self.table.xview)
        xscroll.pack(padx=5,pady=5,side='bottom',fill='both')
        self.table.configure(xscrollcommand=xscroll.set)
        self.table.bind("<Control-MouseWheel>", lambda e : self.table.xview_scroll(int(-1*(e.delta)), "units"))

    def _on_save_to_json(self, level=1):
        pass

    def _export_to_json(self):
        pass

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
        self.tabs.bind("<Double-1>",self.remove_tab)
        self.tabs.pack(padx=10,pady=10,fill='both',expand=True)

        self.empty_frame = ttk.Frame()
        self.tabs.add(self.empty_frame,text="+")


        # Status bar
        self.bottom_bar = ttk.Frame()
        self.bar_label = ttk.Label(self.bottom_bar,text=str(build_connection()))
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
        self.menu.add_cascade(label="Settings", menu=setting_menu)

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

    def remove_tab(self, e):
        index = self.tabs.index(self.tabs.select())
        self.tabs.forget(index)


if __name__ == "__main__":
    build_connection()
    app = App()
    app.iconbitmap("logo/logo-sea.ico")
    app.title("Seabank - Finance Project")
    app.mainloop()