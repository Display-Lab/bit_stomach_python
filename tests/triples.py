import warnings
import time
import logging


import sys
import warnings
import time
import logging
import json
import re
import numpy as np 
import matplotlib.pyplot as plt 

import pandas as pd
from rdflib import Graph, Literal, Namespace, URIRef,BNode
from rdflib.collection import Collection
from rdflib.namespace import FOAF, RDF, RDFS, SKOS, XSD
from rdflib.serializer import Serializer
from rdfpandas.graph import to_dataframe
from SPARQLWrapper import XML, SPARQLWrapper
from calc_gaps_slopes import gap_calc,trend_calc,monotonic_pred,mod_collector
warnings.filterwarnings("ignore")
measure_dicts={}
social_dicts={}
goal_dicts={}
social_comparison_dicts={}
goal_comparison_dicts={}
def read(file):
    #start_time = time.time()
    g = Graph()
    g.parse(data=file,format="json-ld")
    #logging.critical(" reading graph--- %s seconds ---" % (time.time() - start_time)) 
    return g

f=open(sys.argv[1])
spek_cs = json.load(f)
#print(type(content))
spek_cs =json.dumps(spek_cs)
a=read(spek_cs)
f = open("test1.txt", "w")
for triples in a.triples((None, None, None)):
    f.write(str(triples))
    f.write("\n")
