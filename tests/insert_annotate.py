import warnings
import time
import logging

import pandas as pd
import numpy as np
import scipy
from scipy import stats
from rdflib import Graph, Literal, Namespace, URIRef,BNode
from rdflib.collection import Collection
from rdflib.namespace import FOAF, RDF, RDFS, SKOS, XSD
from rdflib.serializer import Serializer
from rdfpandas.graph import to_dataframe
from SPARQLWrapper import XML, SPARQLWrapper

s=URIRef("http://example.com/app#display-lab")
p=URIRef('http://example.com/slowmo#IsAboutMeasure')

def insert_annotate(a,mod_df,goal_dicts,social_dicts):

    #insert performer graph for candidate smasher
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
    
    
    #iterate through mod_df
    for rowIndex, row in mod_df.iterrows():  # iterate over rows
        
        if(row['goal_gap']=="positive"or row['goal_gap']=="negative"):
            #print(row['Measure_Name'])
            ac=BNode(row['Measure_Name'])
            av=goal_dicts.get(ac)
            goal_gap_size=row['goal_gap_size']
            goal_gap_size=Literal(goal_gap_size)
            #annotate goal comparator
            o14=BNode() 
            a.add((s14,p14,o14))
            a=annotate_goal_comparator(a,o14,ac,av)
            #annotate performance gap for goal comparator
            o14=BNode() 
            a.add((s14,p14,o14))
            a=annotate_performance_goal_gap(a,o14,ac,av)
            #annotate positive gap for goal comparator
            if(row['goal_gap']=="positive"):
                o14=BNode() 
                a.add((s14,p14,o14))
                a=annotate_positive_goal_gap(a,o14,ac,av,goal_gap_size)
            #annotate negative gap for goal comparator
            if(row['goal_gap']=="negative"):
                o14=BNode() 
                a.add((s14,p14,o14))
                a=annotate_negative_goal_gap(a,o14,ac,av,goal_gap_size)


    #call annotate performance gap content psdo_0000106

    return a
def annotate_goal_comparator(a,s16,measure_Name,o16):
    p15=RDF.type
    o15=URIRef('http://purl.obolibrary.org/obo/psdo_0000094')
    a.add((s16,p15,o15))
    p16=URIRef('http://example.com/slowmo#RegardingComparator')
    a.add((s16,p16,o16))
    p17=URIRef('http://example.com/slowmo#RegardingMeasure')
    o17=measure_Name
    a.add((s16,p17,o17))
    return a

def annotate_performance_goal_gap(a,s16,measure_Name,o16):
    p15=RDF.type
    o15=URIRef('http://purl.obolibrary.org/obo/psdo_0000106')
    a.add((s16,p15,o15))
    p16=URIRef('http://example.com/slowmo#RegardingComparator')
    a.add((s16,p16,o16))
    p17=URIRef('http://example.com/slowmo#RegardingMeasure')
    o17=measure_Name
    a.add((s16,p17,o17))
    return a

def annotate_positive_goal_gap(a,s16,measure_Name,o16,goal_gap_size):
    p15=RDF.type
    o15=URIRef('http://purl.obolibrary.org/obo/psdo_0000104')
    a.add((s16,p15,o15))
    p16=URIRef('http://example.com/slowmo#RegardingComparator')
    a.add((s16,p16,o16))
    p17=URIRef('http://example.com/slowmo#RegardingMeasure')
    o17=measure_Name
    a.add((s16,p17,o17))
    p18=URIRef('http://example.com/slowmo#PerformanceGapSize')
    o18=goal_gap_size
    a.add((s16,p18,o18))
    return a

def annotate_negative_goal_gap(a,s16,measure_Name,o16,goal_gap_size):
    p15=RDF.type
    o15=URIRef('http://purl.obolibrary.org/obo/psdo_0000105')
    a.add((s16,p15,o15))
    p16=URIRef('http://example.com/slowmo#RegardingComparator')
    a.add((s16,p16,o16))
    p17=URIRef('http://example.com/slowmo#RegardingMeasure')
    o17=measure_Name
    a.add((s16,p17,o17))
    p18=URIRef('http://example.com/slowmo#PerformanceGapSize')
    o18=goal_gap_size
    a.add((s16,p18,o18))
    return a