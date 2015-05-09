from snakebite.client import Client

if __name__ == "__main__":
    client = Client("10.214.208.11", 9000, effective_user='hadoop')
    for x in client.ls(['/']):
        print x
    print list(client.count(['/']))
    print client.df()
    print list(client.du(['/'], False, True))
    list(client.touchz(['/test1']))
    print list(client.delete(['/test']))