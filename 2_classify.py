#!/usr/bin/env python3
import os
import csv
import json
import logging
import argparse
from tqdm import tqdm
from googleapiclient import discovery

logging.basicConfig(level=logging.INFO)


def get_messages(fn):
    '''Takes a file with messages as input (either first column of CSV or newline-separated text)
    and returns the messages'''
    if fn[-3:].lower() == 'csv':
        logging.info(f"Opening {fn} as CSV file, assuming first column contains messages")
        with open(fn) as f:
            reader = csv.reader(f)
            return [l[0].strip() for l in reader]
    else:
        logging.info(f"Opening {fn} as text file, assuming one message per line")
        with open(fn) as f:
            return [l.strip() for l in f.readlines()]

        
def build_client(apikey):
    client = discovery.build(
        "commentanalyzer",
        "v1alpha1",
        developerKey=apikey,
        discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
        static_discovery=False,
        cache_discovery=True,)
    return client
        
            
def make_request(message, client):
    '''takes a message and sends to perspective api; returns a dict with toxicity information'''
    analyze_request = {
        'comment': { 'text': message },
        'requestedAttributes': {'TOXICITY': {}},
        "doNotStore": True,}
    try:  
        response = client.comments().analyze(body=analyze_request).execute()
        returnvalue = ({"message": message, "response": response})
    except Exception as e:
        returnvalue = ({"message": message, "response": str(e)})
    return returnvalue



if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Estimate text toxicity with Perspective API")
    parser.add_argument('inputfile', help='Newline-separated text file with URLs OR a csv file with the URLs in the first column.')
    parser.add_argument('outputfile', help='JSON file to write the output to')
    parser.add_argument('--key', help="API key for Perspective. Alternatively, set environment variable PERSPECTIVEKEY")

    args = parser.parse_args()
    
    if args.key:
        apikey = args.key
    else:
        apikey = os.environ.get("PERSPECTIVEKEY", None)
        if apikey==None:
            apikey=input("You did not provide an API key. Try again or paste it here: ")
    
    
    client = build_client(apikey)
    messages = get_messages(args.inputfile)

    print(f"About to classify {len(messages)} messages.")
    cont = input("\n\nType 'yes' to continue: ")

    if cont.lower() == 'yes':
        with open(args.outputfile, mode='w') as fo:
            for message in tqdm(messages): 
                classification = make_request(message, client)
                try:
                    j=json.dumps(classification)
                    fo.write(j)
                    fo.write('\n')
                except:
                    print(classification)
                    logging.warning("Couldn't write the line above. Skipping this entry.")

    else:
        print('stopped by user.')