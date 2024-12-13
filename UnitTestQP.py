from QueryProcessor.QueryProcessor import *
qp = QueryProcessor('database1')

print("select * from users natural join users_membership where users.id_user<=10")
qp.execute_query("select * from users natural join users_membership where users.id_user<=10",0)
print("select * from users join products on users.id_user=products.product_id where users.id_user<=10")
qp.execute_query("select * from users join products on users.id_user=products.product_id where users.id_user<=10",0)