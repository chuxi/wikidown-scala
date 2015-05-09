from hdfs.ext.kerberos import KerberosClient

if __name__ == "__main__":
    client = KerberosClient("http://10.214.208.11:9000")
    client.list("/")
    pass