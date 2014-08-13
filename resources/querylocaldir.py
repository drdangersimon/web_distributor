#from flask.ext.restful import Resource
from glob import glob
import os
import numpy as np


class GalDistribute(object):
    '''Stores and distributes names and spectra of galaxies that need to be
    processed'''
    


    def initalize(self, path='/home/thuso/Phd/LRGs/Stacked_SDSS/stacks',
                  outpath='/home/thuso/Phd/experements/hierarical/LRG_Stack/stacked_real'):
        '''Initalizes io operations. Loads data, and makes place to
        save output when done'''
        
        # initalize parameters
        self.spectra = {}
        self.results = {}
        # load data
        spectra = glob(os.path.join(path,'*.vespa'))
        for spec in spectra:
            name = spec.split('/')[-1]
            redshift, data = vespa2array(spec)
            self.spectra[name] = data.copy()
            self.results[name] = {'running':False, 'results':False,
                                  'redshift': redshift}
        self.next_name = self._generate_gal
        # make results saving path
        
    def _generate_gal(self):
        '''generator for spectra names'''
        for spec in self.spectra:
            yield spec

def vespa2array(path):
    '''Turns a .vespa files into a numpy.array. First row is redshift'''
    with open(path) as f:
        redshift = float(f.readline())
        data = []
        for line in f:
            data.append(np.float64(line.split()))
        data = np.asarray(data)
        # Remove 0's
        data = data[data[:,1].nonzero(),:]
        return redshift, data
