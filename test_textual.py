from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Label, DirectoryTree,  Input, DataTable, Static, Button, TextArea,TabbedContent, Tabs, TabPane
from textual.containers import Container, Vertical, Horizontal, HorizontalScroll, VerticalScroll, ScrollableContainer
from textual.screen import Screen
from textual import on
from textual.message import Message

class FileScreen(VerticalScroll):
    def compose(self) -> ComposeResult:
        yield DirectoryTree(path=".")

class DirectorySelected(DirectoryTree):
    def on_directory_selected(self):
        self.notify(self.path)

class LogFooter(Footer):
    def __init__(self, id):
        super().__init__()
        self.id = id

    def compose(self) -> ComposeResult:
        with TabbedContent("Log", initial="log"):
            with TabPane("log", id="log"):
                yield TextArea(disabled=True)
            with TabPane("output", id="output"):
                yield TextArea(disabled=True)

class BorderLabel(Label):
    def __init__(self):
        super().__init__(self)

    def on_mount(self, title):
        self.border_title = title

class ModelTable(DataTable):
    MODEL_COLUMNS = [("id", "product", "y", "x1", "x2", "corr_x1", "corr_x2", "inrcpt.", "coeff_x1", "coeff_x2", "r2_score", "mape", "mse")]
    CSS_PATH = 'main.tcss'
    
    def __init__(self):
        super().__init__()
        self.box = None
        self.show_edge = False
        self.zebra_stripes = True
        self.cursor_type = 'row'
        self.fixed_columns = 1
        self.expand = True
        
    def on_mount(self) -> None:
        label = self.query_one(Label)
        label.border_title = "Model"
        
        self.add_columns(*self.MODEL_COLUMNS[0])
        for i in range(180):
            self.add_row(i, f"Status {i}")

class DynamicTable(DataTable):
    MODEL_COLUMNS = [("id", "product", "y", "x1", "x2", "corr_x1", "corr_x2", "inrcpt.", "coeff_x1", "coeff_x2", "r2_score", "mape", "mse")]
    
    def __init__(self):
        super().__init__()
        self.box = None
        self.show_edge = False
        self.zebra_stripes = True
        self.cursor_type = 'row'
        self.fixed_columns = 1
        self.expand = True
        
        self._init_data()

    def on_mount(self):
        self.add_columns(self.MODEL_COLUMNS[0])
        for i in range(180):
            self.add_row(i, f"Status {i}")

class MainScreen(Screen):
    CSS_PATH = 'main.tcss'

    def compose(self):
        yield FileScreen(id="filenav")
        yield Static(id="static1")
        yield Static(id="static2")

class FirstScreen(Screen):
    def compose(self) -> ComposeResult:
        yield Header(id="Header")
        yield Footer(id="Footer")
        yield FileScreen()

MODEL_ROWS = [("No", "Product", "Y", "X1", "X2", "Int.", "Coef. X1", "Coef. X2", "p_val X1", "p_val X2", "r2score", "MAPE", "MSE"),
("1", "BCL_12", "ODR_YOY", "GDP", "CPI", "0.01", "0.02", "0.02", "0.05", "0.05", "0.9", "0.10", "0.0001"),
("2", "BCL_12", "ODR_YOY", "CPI_GROWTH", "GDP_GROWTH", "0.01", "0.02", "0.02", "0.05", "0.05", "0.9", "0.10", "0.0001"),
("3", "BCL_12", "ODR_YOY", "GDP_Q1", "CPI_DELTA", "0.01", "0.02", "0.02", "0.05", "0.05", "0.9", "0.10", "0.0001"),]

class ModelSection(Static):
    def compose(self) -> ComposeResult:
        yield ModelTable()

class TabScreen(Container):
    def compose(self):
        """Create child widgets for the app."""
        # yield ScrollableContainer(ModelSection())
        with TabbedContent("Models", initial="io"):
            with TabPane("IO", id="io"):
                yield Input()
            with TabPane("Models", id="Models"):
                yield Button(id="run", label="Run")
                yield ScrollableContainer(ModelSection())

    # @on(Input.Submitted)
    # def action_input_submit(self):
    #     input = self.query_one(Input)
    #     text = input.value
    #     self.mount(Label(text))
    #     self.post_message(text)
    #     input.value = ''

class LogScreen(Container):
    def compose(self):
        with TabbedContent("Log", initial="log"):
            with TabPane("log", id="log"):
                yield TextArea(disabled=True)
            with TabPane("output", id="output"):
                yield TextArea(disabled=True)

    def action_input_submit(self, message: Message):
        input = self.query_one(Input)
        text = input.value
        self.mount(Label(text))
        input.value = ''

class SecondScreen(Container):
    def compose(self):
        yield TabScreen()

class Main(App):
    """A Textual app to manage stopwatches."""

    BINDINGS = [("d", "toggle_dark", "Toggle dark mode")]
    TITLE = "Seabank Seacaster"
    # CSS_PATH = "./main.tcss"'

    def compose(self) -> ComposeResult:
        yield TabScreen()
        yield LogFooter(id="log_footer")

    def on_tab_screen_input_submit(self):
        ...
        
    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark

if __name__ == "__main__":
    app = Main()
    app.run()