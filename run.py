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
        # other varibles
        self.done = False
        
    def initalize(self):
        '''starts all sockets'''
        # send
        self.gal_send = self.context.socket(zmq.PUSH)
        self.gal_send.bind("tcp://127.0.0.1:%i"%self.sendport)
        self.gal_send.setsockopt(zmq.IDENTITY, b'gal send')
        # recive results
        self.results_receiver = self.context.socket(zmq.PULL)
        self.results_receiver.bind("tcp://127.0.0.1:%i"%self.recivport)
        self.results_receiver.setsockopt(zmq.IDENTITY, b'gal recive')
        # manage
        self.control_sender = self.context.socket(zmq.PUB)
        self.control_sender.bind("tcp://127.0.0.1:%i"%self.manageport)
        self.control_sender.setsockopt(zmq.IDENTITY, b'gal manage')
        # make poll
        self.poller = zmq.Poller()
        self.poller.register(self.results_receiver, zmq.POLLIN)
        #
        

        # load in galaxies

    def start(self):
        '''Starts distributing data from input directories'''
        while True:
            time.sleep(1)
            # check results
            socks = dict(self.poller.poll(1000))           
            if self.results_receiver in socks:
                # get results
                msg = None
                msg, id, py_obj = self.results_receiver.recv_pyobj(zmq.NOBLOCK)
                if msg == 'need data' and not self.done:
                    # Send data and record worker
                    print 'sending data to %s'%id
                    data = self.get_next()
                    self.update(id, 0)
                    self.gal_send.send_pyobj((id, data))
                elif msg == 'status':
                    # log status
                    print 'update from %s'%id
                    self.update(id, py_obj)
                elif msg == 'done':
                    # get results and send more data or tell processes to finish
                     print '%s finished'%id
                     self.finalize(id, py_obj)
            # check if finished
            if self.done:
                self.close()
            
 
    def save(self):
        pass
               
    def get_next(self):
        '''Gets next data to send and checks how many gal are left'''
        print 'this many are left'
        self.done = True
        return None, None
    def update(self, id, ess, chains=None):
        '''Keeps track of all data that is being processed and prints message
        about status'''
    def finalize(self, id, chains):
        '''When data is finished. Saves and removes from working list'''
        pass    
    def close(self):
        '''Send mesgage for all processers to close'''
        self.control_sender.send('exit')

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
        # gets data to work on
        self.work_receiver = self.context.socket(zmq.PULL)
        self.work_receiver.connect("tcp://%s:%i"%(self.host_addr, self.recivport))
        self.work_receiver.setsockopt(zmq.IDENTITY, self.id )
        # send results and status
        self.results_sender = self.context.socket(zmq.PUSH)
        self.results_sender.connect("tcp://%s:%i"%(self.host_addr, self.sendport))
        self.results_sender.setsockopt(zmq.IDENTITY, self.id )
        # gets info when to quit
        self.control_receiver = self.context.socket(zmq.SUB)
        self.control_receiver.connect("tcp://%s:%i"%(self.host_addr,self.manageport))
        self.control_receiver.setsockopt(zmq.SUBSCRIBE, '')
        # make poller
        self.poller = zmq.Poller()
        self.poller.register(self.work_receiver, zmq.POLLIN)
        self.poller.register(self.control_receiver, zmq.POLLIN)
        
    def get_data(self):
        '''Reqests data for processing'''
        # tell client that it's ready
        self.results_sender.send_pyobj((b'need data', self.id, []))
        # check is all work is done
        done = self.check_done()
        if done:
            return None, None
        while True:
            try:
                id, pyobj= self.work_receiver.recv_pyobj(zmq.NOBLOCK)
                if id == self.id:
                    data, param = pyobj
                    break
            except zmq.Again:
                print 'trouble reciving data try again in 5 seconds'
                time.sleep(5)
            
        print 'recived_data'
        return data, param
    
    def send_update(self, ess):
        '''Sends effective sample size to client'''
        # send client report
        self.results_sender.send_pyobj(('status',self.id, ess))

    def send_results(self, results):
        '''When done sends results'''
        # send results
        self.results_sender.send_pyobj(('done', self.id, results))
        # check if need to quit

    def check_done(self):
        '''Checks if I should finish'''    
        socket = self.poller.poll(1000)
        if self.control_receiver in socket:
            msg = self.control_receiver.recv(zmq.NOBLOCK)
            if msg == 'exit':
                print 'Finishing'
                return True
        return False
    
def test_worker():
    '''Stars worker and does tests'''
    print 'starting worker'
    client = Client('127.0.0.1')
    data, param = client.get_data()
    client.send_update(999)
    client.send_results((data,param,[]))
    print 'Done'

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

