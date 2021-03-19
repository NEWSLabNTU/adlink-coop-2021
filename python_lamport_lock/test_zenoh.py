import zenoh
import time
import datetime

machine_num = 1
my_request_path = '/LCMutex/Request/'+str(machine_num)


own_request_time = None
requesting_flag = False

request_lst = []

total_machine_cnt = 3
ack_list = []

def release():
    global own_request_time, requesting_flag, request_lst, ack_list, w
    own_request_time = None
    requesting_flag = False
    ack_list = []
    #send all deferred requests
    if len(request_lst) != 0:
        for foreign_req in request_lst:
            (f_path, f_time) = foreign_req
            #send ack
            send_ack(w, f_path)
    request_lst = []
    print("Mutex released from machine " +str(machine_num))

def critical_session():
    print("Machine "+str(machine_num)+" entered critical session")
    time.sleep(1)
    print("Releasing mutex")
    release()

def ack_listener(change):
    global total_machine_cnt, total_acks
    if change.value is not None:
        ack_list.append(change.value)
    print("Ack list = ", ack_list)
    if(len(ack_list) == total_machine_cnt-1):
        #execute critical session
        critical_session()


def listener(change):
    global w
    global requesting_flag
    global own_request_time
    global request_lst
    global machine_num
    global my_request_path
    print(">> [Subscription listener] received {:?} for {} : {} with timestamp {}"
            .format(change.kind, change.path, '' if change.value is None else change.value.get_content(), change.timestamp))
    if requesting_flag == False:
        #send ack
        send_ack(w, change.path)
        pass
    elif own_request_time == None and requesting_flag:
        if change.path == my_request_path:
            own_request_time = change.timestamp.time
            print("own time set: ", own_request_time)
            #check if any delayed request due to slow self register
            if len(request_lst) != 0:
                to_delete = []
                for foreign_req in request_lst:
                    (f_path, f_time) = foreign_req
                    if f_time < own_request_time:
                        #send ack
                        send_ack(w, f_path)
                        to_delete.append(foreign_req)
                for itm in to_delete:
                    request_lst.remove(itm)
        else:
            request_lst.append((change.path, change.timestamp.time))
            request_lst.sort(key=lambda tup:tup[1])
    else:
        if change.path != my_request_path:
            if len(request_lst) == 0:
                #no request list
                if change.timestamp.time < own_request_time:
                    #send ack
                    send_ack(w, change.path)
                else:
                    request_lst.append((change.path, change.timestamp.time))
            else:
                request_lst.append((change.path, change.timestamp.time))
                request_lst.sort(key=lambda tup:tup[1])
                if len(request_lst) != 0:
                    to_delete = []
                    for foreign_req in request_lst:
                        (f_path, f_time) = foreign_req
                        if f_time < own_request_time:
                            #send ack
                            send_ack(w, f_path)
                            to_delete.append(foreign_req)
                    for itm in to_delete:
                        request_lst.remove(itm)
        else:
            print("Bug")

def send_request(w):
    global requesting_flag, my_request_path, machine_num
    if requesting_flag == False:
        print("Putting a value at " + my_request_path)
        w.put(my_request_path, 'Hello World! This is machine'+str(machine_num)+' requesting the mutex lock.')
        print('time = ', datetime.datetime.now())
        requesting_flag = True
    else:
        print("Own request pending")

def send_ack(w, req_origin_path):
    global machine_num
    if req_origin_path == my_request_path:
        return
    ack_to_path = "/LCMutex/Ack/" + req_origin_path.split('/')[-1]
    print("Machine "+str(machine_num)+" acking to :", ack_to_path)
    w.put(ack_to_path, str(machine_num))

z = zenoh.Zenoh({})
w = z.workspace('/LCMutex')
sub_req = w.subscribe('/LCMutex/Request/**', listener)
sub_ack = w.subscribe('/LCMutex/Ack/'+str(machine_num), ack_listener)


time.sleep(20)
send_request(w)
time.sleep(10)
send_request(w)
time.sleep(8)
send_request(w)
time.sleep(40)
sub_req.close()
sub_ack.close()
z.close()

