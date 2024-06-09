from pythonnet import clr_loader
import pythonnet

# pythonnet.set_runtime_from_env()
pythonnet.load("coreclr", runtime_config="runtimeconfig.json")

import clr
import sys

clr.AddReference("System.IO")
clr.AddReference("System.Drawing")
clr.AddReference("System.Reflection")
clr.AddReference("System.Threading")
clr.AddReference("System.Windows.Forms")
clr.AddReference("System.Xaml")
# clr.AddReference("System.Windows.Markup")
clr.AddReference("System.Windows")
# clr.AddReference(r"wpf\\PresentationFramework")

import System
import System.IO
import System.Drawing
import System.Reflection
import System.Windows.Forms
# import System.Windows
from System.Threading import ApartmentState, Thread, ThreadStart
# import System.Windows.Markup

(SUCCESS, SYS_ERROR, INIT_ERROR, IO_ERROR, WEB_ERROR) = range(4)

ERROR_CODE = {
    SYS_ERROR: "System Error",
    INIT_ERROR: "Initialization Error",
    IO_ERROR: "IO Operation Error",
    WEB_ERROR: "Web service not available",
}

# native error code
if sys.platform == 'win32':
    import clr
elif sys.platform == 'linux':
    print("Import module for linux")
elif sys.platform == 'ios':
    print("Import module for ios")
elif sys.platform == 'android':
    print("Import module for android")
else:
    raise OS_ERROR

# web error code
if web_server == 'on':
    print("Module web server is on")
else:
    print("Module web server is off")

class InteropExplorer(System.Windows.Forms.Form):
    def __init__(self):
        super().__init__()

        # Set the titlebar name
        self.Text = "Seabank ID"

        # Set the background color
        self.BackColor = System.Drawing.Color.FromArgb(238, 238, 238)

        # Set the default size window when openup
        self.ClientSize = System.Drawing.Size(600, 600)

        caption_height = System.Windows.Forms.SystemInformation.CaptionHeight

        self.MinimumSize = System.Drawing.Size(392, (117 + caption_height))

        # Splitter left
        self.left_splitter = System.Windows.Forms.Splitter()
        self.left_splitter.Location = System.Drawing.Point(221, 0)
        self.left_splitter.Size = System.Drawing.Size(3, 273)
        self.left_splitter.TabIndex = 1
        self.left_splitter.BackColor = System.Drawing.Color.Red

        self.right_splitter = System.Windows.Forms.Splitter()
        self.right_splitter.Dock = System.Windows.Forms.DockStyle.Top

        # Width is irrelevant if splitter is docked to Top.
        self.right_splitter.Height = 3

        # Use a different color to distinguish the two splitters.
        self.right_splitter.BackColor = System.Drawing.Color.Blue
        self.right_splitter.TabIndex = 1

        # Set TabStop to false for ease of use when negotiating UI.
        self.right_splitter.TabStop = False

        # Treeview Explorer
        self.left_treeview = System.Windows.Forms.TreeView()
        self.left_treeview.Dock = System.Windows.Forms.DockStyle.Left
        self.left_treeview.BackColor = System.Drawing.Color.FromArgb(238, 238, 150)
        self.left_treeview.Width = self.ClientSize.Width // (7 // 2)
        self.left_treeview.TabIndex = 0
        self.left_treeview.Nodes.Add("Treeview")

        self.Controls.Add(self.left_treeview)

    def run(self):
        # Setup running instance forms
        System.Windows.Forms.Application.Run(self)

    def Dispose(self):
        self.components.Dispose()
        System.Windows.Forms.Dispose(self)


class Window(System.Windows.Forms.Form):
    def __init__(self):
        stream = System.IO.StreamReader("Test.xaml")
        window = System.Windows.Markup.XamlReader(stream)

        System.Windows.Application().Run(window)


def main_form_thread():
    interop_form = InteropExplorer()

    print("New window created ...")

    wpf = System.Windows.Forms.Application
    wpf.Run(interop_form)

    interop_form.Dispose()

if __name__ == "__main__":
    main_form_thread()
    # thread = Thread(ThreadStart(Window))
    # thread.SetApartmentState(ApartmentState.STA)
    # thread.Start()
    # thread.Join()

    # print(dir(System.Windows.Forms.Form))