import tkinter as tk
from tkinter import BOTH, ttk
from abc import ABC

# import sv_ttk
import platform

class InterfaceTitleBar(ABC):
    def __init__(self):
        self.system = platform.system()
        self.machine = platform.machine()
        self.os = platform.platform()
        self.uname = platform.uname()
        
        self.TITLE_BAR_COLOR = 0x000e0e0e
        self.TITLE_TEXT_COLOR = 0x00FFFFFF
        self.set_title_bar()

    def set_title_bar(self):
        try:
            print(self.system)
            match self.system:
                case "Windows": 
                    from ctypes import windll, byref, sizeof, c_int
                    HWND = windll.user32.GetParent(self.winfo_id())
                    windll.dwmapi.DwmSetWindowAttribute(HWND, 35, byref(c_int(self.TITLE_BAR_COLOR)),sizeof(c_int))
                case "Darwin": pass
                case "Java": pass
                case "Linux": pass

        except Exception as e:
            print(e)

class App(tk.Tk):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.geometry("800x600")
        self.title("Seabank Finpro Dashboard")
        # self.overrideredirect(True)
        # sv_ttk.set_theme("dark")

        self.default_theme()

    def default_theme(self):
        big_frame = ttk.Frame(self)
        big_frame.pack(fill="both", expand=True)

    def title_bar(self):
        frame = tk.Frame(self, bg="black", relief='raised', bd=2)
        close_button = tk.Button(self.frame, text="X", command=self.destroy)

        frame.pack(expand=1, fill=tk.X)
        close_button.pack(expand=1,fill=tk.BOTH)

    def main_frame(self):
        self.open_button = tk.Button(self, text="Open", command=App)
        self.open_button.grid()

class MainFrame(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)
        self.grid()
        self.create_widgets()

    def create_widgets(self):
        self.quit_button = tk.Button(self, text="Quit", command=self.quit)
        self.quit_button.grid()

if __name__ == "__main__":
    app = App()
    app.mainloop()