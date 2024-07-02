
import win32api
import win32gui
import win32con

def wndProc(hWnd, message, wParam, lParam):
    if message == win32con.WM_DESTROY:
        win32api.PostQuitMessage(0)
    else:
        return win32gui.DefWindowProc(hWnd, message, wParam, lParam)


className = 'MyWindowClass'
hInstance = win32api.GetModuleHandle()
wndClass = win32gui.WNDCLASS()
wndClass.style = win32con.CS_HREDRAW | win32con.CS_VREDRAW
wndClass.lpfnWndProc = wndProc
wndClass.hInstance = hInstance
wndClass.hbrBackground = win32gui.GetStockObject(win32con.WHITE_BRUSH)
wndClass.lpszClassName = className
wndClassAtom = win32gui.RegisterClass(wndClass)

hwnd = win32gui.CreateWindow(
    className,
    'My PyWin32 Application',
    win32con.WS_OVERLAPPEDWINDOW,
    win32con.CW_USEDEFAULT,
    win32con.CW_USEDEFAULT,
    win32con.CW_USEDEFAULT,
    win32con.CW_USEDEFAULT,
    win32con.NULL,
    win32con.NULL,
    hInstance,
    None
)

hwndButton = win32gui.CreateWindow(
    'BUTTON',
    'Click me',
    win32con.WS_TABSTOP | win32con.WS_VISIBLE | win32con.WS_CHILD | win32con.BS_DEFPUSHBUTTON,
    50,
    50,
    100,
    30,
    hwnd,
    None,
    hInstance,
    None
)