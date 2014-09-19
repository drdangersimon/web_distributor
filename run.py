import time
import zmq
import socket
from  multiprocessing import Process
import numpy as np

class Server(object):
    def __init__(self, send_path, result_path, sendport=5557, reciveprot=5558,
                 mangeport=5556):
        '''Initalizes server on specific ports to act like flags for telling
           processor what to do.
           
           :param send_path: path to look for galaxies to send

           :param result_path: path to put results once finished. Also will check here when restarting after a crash.

           :param sendport: port to use for sending data to worker

           :param reciveprot: port to recive data from workers

           :param mangeport: port used to track progress of fitting with each different instance.'''
        self.send_path = send_path
        self.result_path = result_path
        self.sendport = int(sendport)
        self.recivport = int(reciveprot)
        self.manageport = int(mangeport)
        self.context = zmq.Context()
        self.cur_workers = {}
        self.initalize()
        
    def initalize(self):
        '''starts all sockets'''
        # send
        self.gal_send = self.context.socket(zmq.DEALER)
        self.gal_send.bind("tcp://127.0.0.1:%i"%self.sendport)
        self.gal_send.setsockopt(zmq.IDENTITY, b'gal send')
        # recive results
        self.results_receiver = self.context.socket(zmq.DEALER)
        self.results_receiver.bind("tcp://127.0.0.1:%i"%self.recivport)
        self.results_receiver.setsockopt(zmq.IDENTITY, b'gal recive')
        # manage
        self.control_sender = self.context.socket(zmq.DEALER)
        self.control_sender.bind("tcp://127.0.0.1:%i"%self.manageport)
        self.control_sender.setsockopt(zmq.IDENTITY, b'gal manage')
        # make poll
        self.poller = zmq.Poller()
        self.poller.register(self.gal_send, zmq.POLLIN|zmq.POLLOUT)
        self.poller.register(self.results_receiver, zmq.POLLIN)
        self.poller.register(self.control_sender, zmq.POLLIN|zmq.POLLOUT)
        

        # load in galaxies

    def start(self):
        '''Starts distributing data from input directories'''
        while True:
            time.sleep(1)
            # check results
            socks = dict(self.poller.poll(1000))           
            if self.gal_send in socks:
                # send galaxy data
                if socks[self.gal_send] == zmq.POLLIN:
                    # recive
                    print 'recive gal'
                    print self.gal_send.recv()
                
                else:
                    #send
                    self.gal_send.send_pyobj(([1],[1,2]))
                    print 'send gal'
                
            if self.results_receiver in socks:
                print 'results'
            if self.control_sender in socks:
                print 'control'
 
    def save(self):
        pass
               
    def send(self, data, param):
        '''send data down a zeromq "PUSH" connection to be processed by 
        listening workers, in a round robin load balanced fashion.'''
        pass
    def update(self):
        pass
    def close(self):
        pass

class Client(object):
    '''
# The "worker" functions listen on a zeromq PULL connection for "work" 
# (numbers to be processed) from the ventilator, square those numbers,
# and send the results down another zeromq PUSH connection to the 
# results manager.
    '''
    def __init__(self, host_addr, reciveport=5557, sendport=5558,
                 mangeport=5556):
        # get identity
        self.id = socket.gethostname() + str(int(round(np.random.rand()*1000)))
        self.sendport = int(sendport)
        self.recivport = int(reciveport)
        self.manageport = int(mangeport)
        self.host_addr = host_addr
        self.context = zmq.Context()
        # initalize contex managers
        self.work_receiver = self.context.socket(zmq.REQ)
        self.work_receiver.connect("tcp://%s:%i"%(self.host_addr, self.recivport))
        self.work_receiver.setsockopt(zmq.IDENTITY, self.id )
        self.results_sender = self.context.socket(zmq.REQ)
        self.results_sender.connect("tcp://%s:%i"%(self.host_addr, self.sendport))
        self.results_sender.setsockopt(zmq.IDENTITY, self.id )
        self.control_receiver = self.context.socket(zmq.REQ)
        self.control_receiver.connect("tcp://%s:%i"%(self.host_addr,self.manageport))
        self.control_receiver.setsockopt(zmq.IDENTITY, self.id )
        # make poller
        self.poller = zmq.Poller()
        self.poller.register(self.work_receiver, zmq.POLLIN|zmq.POLLOUT)
        self.poller.register(self.results_sender, zmq.POLLOUT)
        self.poller.register(self.control_receiver, zmq.POLLIN|zmq.POLLOUT)
        
    def get_data(self):
        '''Reqests data for processing'''
        # tell client that it's ready
        self.work_receiver.send(b'ready')
        data, param = self.work_receiver.recv_pyobj()
        print 'recived_data'
        # check if exit message
        if data is None:
            sys.exit(0)
        return data, param
    
    def send_update(self, ess):
        '''Sends effective sample size to client'''
        # send client report
        self.control_receiver.send_pyobj(ess)

    def send_results(self, results):
        '''When done sends results'''
        # send results
        self.results_sender.send_pyobj(results)

def test_worker():
    '''Stars worker and does tests'''
    print 'starting worker'
    client = Client('localhost')
    data, param = client.get_data()
    print data

def test_server():
    print 'starting server'
    server = Server('','')
    server.start()
    print 'done'
    
if __name__ == "__main__":

    # Fire up our result manager...
    result_manager = Process(target=test_server, args=())
    result_manager.start()

    # Create a pool of workers to distribute work to
    worker_pool = range(1)
    for wrk_num in range(len(worker_pool)):
        Process(target=test_worker, args=()).start()

