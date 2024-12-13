from QueryProcessor.QueryProcessor import *
qp = QueryProcessor('database1')

qp.execute_query("select * from users natural join users_membership where users.id_user<=10",0)