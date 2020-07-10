from multiprocessing import Process


def one(): from MeetUp import consumer
def two(): from MeetUp import producer

Process(target=one).start()
Process(target=two).start()