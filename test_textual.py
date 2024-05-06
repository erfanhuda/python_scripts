from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Label, DirectoryTree,  Input, DataTable, Static, Button, TextArea,TabbedContent, Tabs, TabPane
from textual.containers import Container, Vertical, Horizontal, HorizontalScroll, VerticalScroll
from textual.screen import Screen

class FileScreen(VerticalScroll):
    def compose(self) -> ComposeResult:
        yield DirectoryTree(path=".")

class DirectorySelected(DirectoryTree):
    def on_directory_selected(self):
        self.notify(self.path)

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

class ModelTable(Vertical):
    def __init__(self, title: str) -> None:
        super().__init__()
        self._title= title
        self.styles.height = 10
        self.styles.width = 'auto'

    def _test_dt(self) -> DataTable:
        dt = DataTable()
        dt.box = None
        dt.show_edge= False
        dt.zebra_stripes = True
        dt.cursor_type = 'row'
        dt.fixed_columns = 1
        dt.expand= True
        dt.styles.width = 'auto'
        dt.add_column("id", width=3)
        dt.add_column("product", width=20)
        dt.add_columns("y")
        dt.add_column("x1")
        dt.add_column("x2")
        dt.add_column("intrcept.")
        dt.add_column("corr_x1")
        dt.add_column("corr_x2")
        dt.add_column("coef_x1")
        dt.add_column("coef_x2")
        dt.add_column("r2score")
        dt.add_column("MAPE")
        dt.add_column("MSE")

        for i in range(100):
            dt.add_row(i, f"Status {i}")

        return dt

    def compose(self) -> ComposeResult:
        yield self._test_dt()



class Main(App):
    """A Textual app to manage stopwatches."""

    BINDINGS = [("d", "toggle_dark", "Toggle dark mode")]
    TITLE = "Seabank Seacaster"
    # CSS_PATH = "./main.tcss"'

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        with TabbedContent("Models", initial="io"):
            with TabPane("IO", id="io"):
                yield Input()
            with TabPane("Models", id="Models"):
                yield Button(id="run", label="Run")
                yield ModelTable("Model")

        # with TabbedContent("Log", initial="log"):
        #     with TabPane("log", id="log"):
        #         yield TextArea(disabled=True)
        #     with TabPane("output", id="output"):
        #         yield TextArea(disabled=True)

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark

if __name__ == "__main__":
    app = Main()
    app.run()