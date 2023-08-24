import socket
import requests
import warnings

URL = "mysql://erfa6313_admin:Erfnhd100%@203.175.8.110/erfa6313_erfanhuda"

warnings.filterwarnings("ignore")

def get_ip_local():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('192.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def get_ip_public():
    response = requests.get('https://api64.ipify.org?format=json', verify=False).json()
    return response["ip"]

def get_ip_public_full():
    ip_address = get_ip_public()
    response = requests.get(f'https://ipapi.co/{ip_address}/json/', verify=False).json()
    return response

def ip_checker(url):
    ip_address = socket.gethostbyname(str(url))
    response = requests.get(f'https://ipapi.co/{ip_address}/json/', verify=False).json()
    return response

def get_ip_location(url):
    ip_address = socket.gethostbyname(str(url))
    response = requests.get(f'https://geolocation-db.com/jsonp/{ip_address}', verify=False)
    return response.content.decode()

def scan_ip_range(start, end, ipaddr, port=135):
    if ipaddr is None:
        ipaddr = get_ip_local()

    ip_address = ipaddr.split('.')
    sep = "."
    ip_address = ip_address[0] + sep + ip_address[1] + sep + ip_address[2] + sep
    
    for ip in range(start,end+1):
      addr = ip_address + str(ip)
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      socket.setdefaulttimeout(1)
      result = sock.connect_ex((addr,port))

      if result == 0:
        print(addr, "is live")

def _base_port_scanner(ipaddr:str, ports:str) -> None:
    try:
        sock = socket.socket()
        sock.connect((ipaddr, int(ports)))
        svcv = sock.recv(1024)
        svcv = svcv.decode("utf-8")
        svcv = svcv.strip("/n")

        print(f"Port {str(ports)} is open")
    except ConnectionRefusedError:
        print(f"Port {str(ports)} is closed")

    except UnicodeDecodeError:
        print(f"Port {str(ports)} is closed")

def scan_port(ipaddr: str, ports: str) -> None:
    print("\n","="*20 + f"Scanning {ipaddr}" + "="*20, "\n")
    if "," in ports:
        ports = ports.split(",")
        
        for port in ports:
            _base_port_scanner(ipaddr, port)

    elif "-" in ports:
        ports = ports.split("-")
        start = ports[0]
        end = ports[1]
        ports.clear()

        for port in range(int(start), int(end)+1):
            _base_port_scanner(ipaddr, port)

    else: 
        _base_port_scanner(ports)


IP_PUBLIC = get_ip_public()
IP_LOCAL = get_ip_local()

# print(IP_PUBLIC)
# print(IP_LOCAL)
print(get_ip_public_full())