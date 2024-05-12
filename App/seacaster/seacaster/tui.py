from textual.app import App, Binding, ComposeResult
from textual.widgets import Header, Label, Input, Select, Footer, Static
from textual.widget import Widget
from textual.containers import Container
from textual.screen import Screen

class BorderContainer(Label):
    def __init__(self): 
        super().__init__(self)

class FirstContainer(Static):
    def compose(self) -> ComposeResult:
        yield Container(id="header")
        yield Container(id="content")
        yield Container(id="sidebar")
        yield Container(id="footer")

class Main(App):
    TITLE = "Seacaster GUI"
    BINDINGS = [Binding("d", "toggle_dark", "Toggle dark mode"), Binding("tab", "toggle_sidebar", "Sidebar", show=True, priority=True), Binding("q", "exit", "Exit App", show=True, priority=True)]
    CSS_PATH = 'main.tcss'

    def compose(self):
        yield Header(show_clock=True)
        yield FirstContainer()
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