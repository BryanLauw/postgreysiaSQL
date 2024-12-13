from QueryProcessor.QueryProcessor import *
import socket
class Client:
    def __init__(self, client_id, sock, state):
        self.client_id = client_id
        self.socket = sock
        self.state = state
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("127.0.0.1", 65432))  # Example server address
state = {"transactionId": None, "on_begin": False}
c = Client(0, sock, state)
qp = QueryProcessor('database1',{'c':c})

print("select * from users natural join users_membership where users.id_user<=10")
qp.execute_query("select * from users natural join users_membership where users.id_user<=10 order by nama_user desc limit 3",c)
print("select * from users join products on users.id_user=products.product_id where users.id_user<=10")
qp.execute_query("select * from users join products on users.id_user=products.product_id where users.id_user<=10",c)
print("select * from users join products on users.id_user=products.product_id where users.id_user<=10")
qp.execute_query("select * from users join products on users.id_user=products.product_id where users.id_user<=10",c)

def testUpdateParser():
    parsedQ = QueryProcessor("database1", {'c':c})
    parsedQ.parsedQuery = parsedQ.qo.parse_query('update users set nama_user="a" where id_user = 5', "database1")
    d_write = parsedQ.ParsedQueryToDataWrite()
    if (d_write.table == ["users"] and d_write.column == ["nama_user"] and d_write.new_value == ['a']):
        print("Test passed")

testUpdateParser()
