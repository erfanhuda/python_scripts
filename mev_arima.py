import blpapi


class AuthBloomberg(blpapi.AuthUser):
    def __init__(self, name, ip):
        super().__init__(blpapi.AuthUser)
        self.name = name
        self.ip = ip
        self.createWithLogonName(self.name, self.ip)


def main():
    auth = blpapi.AuthUser().createWithLogonName()
