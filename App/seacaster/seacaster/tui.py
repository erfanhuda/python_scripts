from dataclasses import dataclass
from textual.app import App, Binding, ComposeResult
from textual.widgets import Header, Label, Input, Select, Footer, Static, DataTable, Log, Button
from textual.widget import Widget
from textual.containers import Container, ScrollableContainer, Grid
from textual.screen import Screen, ModalScreen
from textual.message import Message
from textual.events import Click
from textual import on

from typing import Any
from random import randint, random

def title_container(text: str, *args, **kwargs) -> Container:
    container = Container(*args, **kwargs)
    container.border_title = text
    return container

class TopContainer(Screen):
    ROWS = [("key", "value"), ("product", "[BCL, SPL]")]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def compose(self):
        yield DataTable(id="params")
        yield Log()

    def on_mount(self):
        self.border_title = 'Variables'
        
        table = self.query_one("#params", DataTable)
        table.styles.margin = (3, 0)
        table.expand= True
        table.zebra_stripes = True
        table.add_columns(*self.ROWS[0])
        for n, row in enumerate(self.ROWS[1:]):
            table.add_row(*row, label=str(n))

        table.focus()

    def on_data_table_cell_selected(self, event:DataTable.CellSelected) -> None:
        cell_value: Any = event.value
        msg = (f"Original value of cell at {event.coordinate} is {event.value} and type {type(event.value)}\n")
        self.query_one(Log).write(msg)
        self.app.push_screen(EditCellScreen(cell_value))


class EditCellScreen(ModalScreen):
    def __init__(self, cell_value: Any, name: str|None=None, id: str | None=None, classes: str | None=None):
        super().__init__(name=name, id=id, classes=classes)
        self.cell_value = cell_value

    def compose(self) -> ComposeResult:
        yield Input()

    def on_mount(self):
        cell_input = self.query_one(Input)
        cell_input.value = str(self.cell_value)
        cell_input.focus()

    def on_input_submitted(self, e: Input.Submitted) -> None:
        p_screen = self.app.get_screen("top")

        table = p_screen.query_one(DataTable)
        table.update_cell_at(table.cursor_coordinate, e.value, update_width = True)
        msg = (f"Updated value of cell at {table.cursor_coordinate} is {e.value} and type {type(e.value)}\n")
        p_screen.query_one(Log).write(msg)
        self.app.pop_screen()

class PairInput(Static):
    def compose(self) -> ComposeResult:
        yield Input(id='key')
        yield Input(id='value')

class FirstContainer(Static):
    def compose(self) -> ComposeResult:
        # yield Container(id="header")
        # yield Container(id="content")
        yield Container(id="sidebar")
        # yield Container(id="footer")

        # yield title_container("Variables",id="header")
        yield Button("Add", id="add_column")
        with ScrollableContainer(id="input-container"):
            yield PairInput()
            yield PairInput()

class MainLayout(Container):
    BINDINGS = [Binding("a", "add_column", "Add"), Binding("r", "remove_column", "Remove")]
    CSS_PATH = 'main.tcss'
    def compose(self) -> ComposeResult:
        yield Container(id='sidebar')
        yield FirstContainer()
        yield Button("Combinations")

    def action_add_column(self) -> None:
        new_input = PairInput()
        container = self.query_one("#input-container")
        container.mount(new_input)

    def action_remove_column(self) -> None:
        container = self.query("#input-container")
        self._on_unmount(container.last())
        
    def on_mount(self):
        ...
class Main(App):
    TITLE = "Seacaster GUI"
    BINDINGS = [Binding("tab", "toggle_sidebar", "Sidebar", show=True, priority=True), Binding("d", "toggle_dark", "Toggle dark mode"), Binding("a", "add_column", "Add"), Binding("q", "exit", "Exit App")]
    CSS_PATH = 'main.tcss'
    SCREENS = {'top': TopContainer()}

    def compose(self):
        yield Header(show_clock=True)
        yield MainLayout()
        yield Footer()

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark
    
    def action_exit(self) -> None:
        """An action to toggle dark mode."""
        self.exit()

    def action_toggle_sidebar(self) -> None:
        self.query_one('#sidebar').toggle_class('-active')


if __name__ == '__main__':
    app = Main()
    app.run()