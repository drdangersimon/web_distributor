import time
import zmq
from  multiprocessing import Process


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
        self.initalize()
        
    def initalize(self):
        '''starts all sockets'''
        # send
        self.gal_send = self.context.socket(zmq.PUSH)
        self.gal_send.bind("tcp://127.0.0.1:%i"%self.sendport)
        # recive results
        self.results_receiver = self.context.socket(zmq.PULL)
        self.results_receiver.bind("tcp://127.0.0.1:%i"%self.recivport)
        # manage
        self.control_sender = self.context.socket(zmq.PUB)
        self.control_sender.bind("tcp://127.0.0.1:%i"%self.manageport)
        # load in galaxies

    def start(self):
        '''Starts distributing data from input directories'''
        while True:
            # check results
            todo = self.result_manager()
            # check output
            if todo == 'data':
                # send data
                self.send()
            elif todo == 'status':
                # recive status
                self.update()
            elif todo == 'done':
                # recive finish work and save
                self.save()
            # check if all data is done
            if self.done == self.len_data:
                # send kill
                self.close()
                break
    def save(self):

               
    def send(self):
        '''send data down a zeromq "PUSH" connection to be processed by 
        listening workers, in a round robin load balanced fashion.'''
        # get next galaxy to send

        #send
        self.gal_send.send_pyobj(work_message)
    
        time.sleep(1)

    def update(self):
        # Initialize a zeromq context
        context = zmq.Context()
    
        # Set up a channel to receive results
        results_receiver = context.socket(zmq.PULL)
        results_receiver.bind("tcp://127.0.0.1:5558")

        # Set up a channel to send control commands
 
        for task_nbr in range(10000):
            result_message = results_receiver.recv_json()
            print "Worker %i answered: %i" % (result_message['worker'], result_message['result'])

        # Signal to all workers that we are finsihed
        control_sender.send("FINISHED")
        time.sleep(5)
    def close(self):
        control_sender.send("FINISHED")

class Client(object):
    '''
# The "worker" functions listen on a zeromq PULL connection for "work" 
# (numbers to be processed) from the ventilator, square those numbers,
# and send the results down another zeromq PUSH connection to the 
# results manager.
    '''
    def __init__(self, host_addr, reciveport=5557, sendport=5558,
                 mangeport=5556):):
        
        self.sendport = int(sendport)
        self.recivport = int(reciveprot)
        self.manageport = int(mangeport)
        self.host_addr = host_addr
        self.context = zmq.Context()
        # initalize contex managers
        self.work_receiver = self.context.socket(zmq.PULL)
        self.work_receiver.connect("tcp://%s:%i"%(self.host_addr, self.recivport))
        self.results_sender = self.context.socket(zmq.PUSH)
        self.results_sender.connect("tcp://%s:%i"%(self.host_addr, self.sendport))
        self.control_receiver = self.context.socket(zmq.SUB)
        self.control_receiver.connect("tcp://%s:%i"%(self.host_addr,self.manageport))

        control_receiver.setsockopt(zmq.SUBSCRIBE, "")
        self. control_receiver
    def get_data(self):
        pass
    def send_update(self):
        pass
    def send_results(self)
    def worker(self, wrk_num):
        # Initialize a zeromq context
        
    
        

    
        # Set up a poller to multiplex the work receiver and control receiver channels
        poller = zmq.Poller()
        poller.register(work_receiver, zmq.POLLIN)
        poller.register(control_receiver, zmq.POLLIN)

        # Loop and accept messages from both channels, acting accordingly
        while True:
            socks = dict(poller.poll())

            # If the message came from work_receiver channel, square the number
            # and send the answer to the results reporter
            if socks.get(work_receiver) == zmq.POLLIN:
                work_message = work_receiver.recv_json()
                product = work_message['num'] * work_message['num']
                answer_message = { 'worker' : wrk_num, 'result' : product }
                results_sender.send_json(answer_message)

            # If the message came over the control channel, shut down the worker.
            if socks.get(control_receiver) == zmq.POLLIN:
                control_message = control_receiver.recv()
                if control_message == "FINISHED":
                    print("Worker %i received FINSHED, quitting!" % wrk_num)
                    break


if __name__ == "__main__":

    # Create a pool of workers to distribute work to
    worker_pool = range(10)
    for wrk_num in range(len(worker_pool)):
        Process(target=worker, args=(wrk_num,)).start()

    # Fire up our result manager...
    result_manager = Process(target=result_manager, args=())
    result_manager.start()

    # Start the ventilator!
    ventilator = Process(target=ventilator, args=())
    ventilator.start()
