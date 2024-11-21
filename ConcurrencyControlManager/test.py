from main import *

cm = ConcurrencyControlManager()


def run_action(transactin_id: int, object: Row, action: Action):
    res = cm.validate_object(object, transactin_id, action)
    if res == False:
        raise "Not allowed"


r = lambda id, obj: run_action(id, obj, "read")
w = lambda id, obj: run_action(id, obj, "write")


def test1():
    a = Row()
    b = Row()
    c = Row()

    t1 = cm.begin_transaction()
    t2 = cm.begin_transaction()

    try:
        r(t1, a)
        w(t2, a)
        r(t1, b)
        w(t1, c)
        w(t2, c)
        print("Allowed")
    except Exception:
        print("Not allowed")


def test2():
    a = Row()
    b = Row()
    c = Row()

    t1 = cm.begin_transaction()
    t2 = cm.begin_transaction()

    try:
        r(t1, a)
        w(t2, a)
        r(t1, b)
        w(t2, c)
        w(t1, c)
        print("Allowed")
    except Exception:
        print("Not allowed")


test1()
test2()
