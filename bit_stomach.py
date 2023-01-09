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
s=URIRef("http://example.com/app#display-lab")
p=URIRef('http://example.com/slowmo#IsAboutMeasure')
p1=URIRef("http://example.com/slowmo#WithComparator")
p3=URIRef('http://schema.org/name')
o5=URIRef("http://purl.obolibrary.org/obo/psdo_0000095")
p5=RDF.type
p6=URIRef("http://example.com/slowmo#ComparisonValue")
for s,p,o in a.triples((s, p, None)):
    s1=o
    for s2,p2,o2 in a.triples((s1,p1,None)):
        measure_dicts[s1]=o2
        s3=o2
        for s4,p4,o4 in a.triples((s3,p3,None)):
            if str(o4)=="peers":
                social_dicts[s1]=o2
                s5=s3
                f.write(str(s5))
                # a.add((s5,p5,o5))
                for s7,p7,o7 in a.triples((s3,p6,None)):
                    social_comparison_dicts[s1]=o7
            if str(o4)=="goal":
                goal_dicts[s1]=o2
                for s8,p8,o8 in a.triples((s3,p6,None)):
                    goal_comparison_dicts[s1]=o8
       
            f.write("\n")
f.close()        
B=a.serialize(format='json-ld', indent=4)
performance_data_df = pd.read_csv(sys.argv[2])

goaldf=pd.DataFrame(goal_comparison_dicts.items())
#socialdf=pd.DataFrame(social_comparison_dicts.items())

goaldf.columns =['Measure_Name', 'comparison_value']
#socialdf.columns=['Measure_Name', 'comparison_value']
goaldf.insert(1, 'comparison_type', 'goal')
#socialdf.insert(1, 'comparison_type', 'peers')

# goaldf.to_csv("goal.csv")
# socialdf.to_csv("social.csv")
# frames = [goaldf, socialdf]
# result = pd.concat(frames)
# result1=result.reset_index(drop=True)
#performance_data_df.to_csv("comparison.csv")
mod_df=mod_collector(performance_data_df, goaldf)
# mod_df.to_csv("mod_df.csv")





s12 = URIRef('http://example.com/app#display-lab')
p12=URIRef('http://example.com/slowmo#IsAboutPerformer')
o12=BNode('p1')
a.add((s12,p12,o12))
s13=BNode('p1')
p13=RDF.type
o13=URIRef("http://purl.obolibrary.org/obo/psdo_0000085")
a.add((s13,p13,o13))
s14=s13
p14=URIRef('http://purl.obolibrary.org/obo/RO_0000091')
o14=BNode()
a.add((s14,p14,o14))
s15=o14
p15=RDF.type
o15=URIRef('http://purl.obolibrary.org/obo/psdo_0000106')
a.add((s15,p15,o15))
for key, value in social_comparison_dicts.items():
    print(key, ' : ', value)
for key, value in goal_comparison_dicts.items():
    print(key, ' : ', value)

for s,p,o in a.triples((s, p, None)):
    s1=o
    for s2,p2,o2 in a.triples((s1,p1,None)):
        o16=list(goal_dicts.values())[0]
        
        s16=o14
        p16=URIRef('http://example.com/slowmo#RegardingComparator')
        a.add((s16,p16,o16))
C=a.serialize(format='json-ld', indent=4)
f1 = open("test12.json", "w")
f1.write(C)    
f1.close()
