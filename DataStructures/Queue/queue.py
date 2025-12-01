from DataStructures.List import array_list as lt

def new_queue():
    return lt.new_list()

def is_empty(queue):
    return lt.is_empty(queue)

def size(queue):
    return lt.size(queue)

def enqueue(queue, element):
    queue = lt.add_last(queue, element)
    return queue

def dequeue(queue):
    queue = lt.remove_first(queue)
    return queue

def peek(queue):
    return lt.get_element(queue, 0)