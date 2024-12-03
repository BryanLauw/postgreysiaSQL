from typing import Union
from time import sleep
from ConcurrencyControlManager import (
    Action,
    ConcurrencyControlManager,
    Response,
    Row,
)
from threading import Lock, Thread

log_mutex = Lock()
cm = ConcurrencyControlManager()


type AtomicAction = tuple[Action, Row, int]
type Transaction = list[AtomicAction]

r = [Row(), Row(), Row()]

trs: list[Transaction] = [
    [
        ("read", r[0], 1),
        ("write", r[1], 2),
        ("read", r[0], 1),
        ("write", r[0], 2),
        ("read", r[2], 0),
    ],
    [
        ("read", r[1], 0),
        ("read", r[0], 1),
        ("write", r[2], 3),
        ("read", r[1], 0),
        ("write", r[2], 1),
    ],
    [
        ("write", r[0], 2),
        ("read", r[2], 0),
        ("read", r[1], 0),
        ("write", r[0], 1),
        ("read", r[1], 0),
    ],
    [
        ("read", r[0], 1),
        ("read", r[1], 0),
        ("write", r[1], 2),
        ("read", r[0], 1),
        ("write", r[2], 1),
    ],
]


def log(v: str):
    with log_mutex:
        print(v)


def runThread(tr: Transaction):
    succeed = False

    while not succeed:
        ts = cm.begin_transaction()

        response: Union[Response, None] = None
        succeed = True
        for op in tr:
            response = cm.validate_object(op[1], ts, op[0])

            # If action not allowed, abort
            if not response.allowed:
                succeed = False
                log(str(ts) + ": ABORT " + str(trs.index(tr)))
                break

            # If allowed, run that action
            log(
                str(ts)
                + ": "
                + str(trs.index(tr))
                + " "
                + str(op[0])
                + " "
                + str(r.index(op[1]))
            )
            sleep(op[2] * 0.01)

        if response != None and not succeed:
            with response.condition:
                response.condition.wait()
        else:
            cm.end_transaction(ts)
            log(str(ts) + ": COMMIT " + str(trs.index(tr)))

    pass


threads: list[Thread] = []
for tr in trs:
    threads.append(Thread(target=runThread, args=[tr]))

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()
