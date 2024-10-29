import numpy as np
import pandas as pd
import os
import dotenv
import requests
import json
import psycopg
from sqlalchemy import create_engine
from bs4 import BeautifulSoup

class contrans:
        def __init__(self):
                dotenv.load_dotenv()
                self.mypassword = os.getenv('mypassword')
                self.congresskey = os.getenv('congresskey')
                self.newskey = os.getenv('newskey')

        def get_votes(self):
                url = 'https://voteview.com/static/data/out/votes/HS118_votes.csv'
                votes = pd.read_csv(url)
                return votes
        
        def get_ideology(self):
                url = 'https://voteview.com/static/data/out/members/H118_members.csv'
                members = pd.read_csv(url)
                return members
        def get_useragent(self):
                url = 'https://httpbin.org/user-agent'
                r = requests.get(url)
                useragent = json.loads(r.text)['user-agent']
                return useragent
        
        def make_headers(self,  email='ntb4su@virginia.edu'):
                useragent=self.get_useragent()
                headers = {
                        'User-Agent': useragent,
                        'From': email
        }
                return headers
        def get_bioguideIDs(self):
                params = {'api_key': self.congresskey,
                                'limit': 1} 
                headers = self.make_headers()
                root = 'https://api.congress.gov/v3'
                endpoint = '/member'
                r = requests.get(root + endpoint,
                                        params=params,
                                        headers=headers)
                totalrecords = r.json()['pagination']['count']
                
                params['limit'] = 250
                j = 0
                bio_df = pd.DataFrame()
                while j < totalrecords:
                        params['offset'] = j
                        r = requests.get(root + endpoint,
                                                params=params,
                                                headers=headers)
                        records = pd.json_normalize(r.json()['members'])
                        bio_df = pd.concat([bio_df, records])
                        j = j + 250
                return bio_df
        def get_bioguide(self, name, state=None, district=None):
        
                members = self.get_bioguideIDs() 

                members['name'] = members['name'].str.lower().str.strip()
                name = name.lower().strip()

                tokeep = [name in x for x in members['name']]
                members = members[tokeep]

                if state is not None:
                        members = members.query('state == @state')

                if district is not None:
                        members = members.query('district == @district')
                
                return members.reset_index(drop=True)
        def get_sponsoredlegislation(self, bioguideid):

                params = {'api_key': self.congresskey,
                          'limit': 1} 
                headers = self.make_headers()
                root = 'https://api.congress.gov/v3'
                endpoint = f'/member/{bioguideid}/sponsored-legislation'
                r = requests.get(root + endpoint,
                                 params=params,
                                 headers=headers)
                totalrecords = r.json()['pagination']['count']
                
                params['limit'] = 250
                j = 0
                bills_list = []
                while j < totalrecords:
                        params['offset'] = j
                        r = requests.get(root + endpoint,
                                         params=params,
                                         headers=headers)
                        records = r.json()['sponsoredLegislation']
                        bills_list = bills_list + records
                        j = j + 250

                return bills_list
        def get_billdata(self, billurl):
                r = requests.get(billurl,
                                params = {'api_key': self.congresskey})
                bill_json = json.loads(r.text)
                texturl = bill_json['bill']['textVersions']['url']
                r = requests.get(texturl,
                                params = {'api_key': self.congresskey})
                toscrape =json.loads(r.text)['textVersions'][0]['formats'][0]['url']
                r = requests.get(toscrape)
                mysoup = BeautifulSoup(r.text, 'html.parser')
                billtext = mysoup.text 
                bill_json['bill_text'] = billtext
                return bill_json
        def make_cand_table(self, members):
                # members is output of get_terms()
                replace_map = {'Republican': 'R','Democratic': 'D','Independent': 'I'}
                members['partyletter'] = members['partyName'].replace(replace_map)
                members['state'] = members['state'].replace(self.us_state_to_abbrev)
                members['district'] = members['district'].fillna(0)
                members['district'] = members['district'].astype('int').astype('str')
                members['district'] = ['0' + x if len(x) == 1 else x for x in members['district']]
                members['district'] = [x.replace('00', 'S') for x in members['district']]
                members['DistIDRunFor'] = members['state']+members['district']
                members['lastname']= [x.split(',')[0] for x in members['name']]
                members['firstname']= [x.split(',')[1] for x in members['name']]
                members['name2'] = [ y.strip() + ' (' + z.strip() + ')' 
                                for y, z in 
                                zip(members['lastname'], members['partyletter'])]
                
                cands = pd.read_csv('data/CampaignFin22/cands22.txt', quotechar="|", header=None)
                cands.columns = ['Cycle', 'FECCandID', 'CID','FirstLastP',
                                'Party','DistIDRunFor','DistIDCurr',
                                'CurrCand','CycleCand','CRPICO','RecipCode','NoPacs']
                cands['DistIDRunFor'] = [x.replace('S0', 'S') for x in cands['DistIDRunFor']]
                cands['DistIDRunFor'] = [x.replace('S1', 'S') for x in cands['DistIDRunFor']]
                cands['DistIDRunFor'] = [x.replace('S2', 'S') for x in cands['DistIDRunFor']]
                cands['name2'] = [' '.join(x.split(' ')[-2:]) for x in cands['FirstLastP']]
                cands = cands[['CID', 'name2', 'DistIDRunFor']].drop_duplicates(subset=['name2', 'DistIDRunFor'])
                crosswalk = pd.merge(members, cands, 
                     left_on=['name2', 'DistIDRunFor'],
                     right_on=['name2', 'DistIDRunFor'],
                     how = 'left')
                return crosswalk
        def terms_df(self, members):
                termsDF = pd.DataFrame()
                for index, row in members.iterrows():
                        bioguide_id = row['bioguideId']
                        terms = row['terms.item']
                        df = pd.DataFrame.from_records(terms)
                        df['bioguideId'] = bioguide_id
                        termsDF = pd.concat([termsDF, df])
                members = members.drop('terms.item', axis=1)
                return termsDF, members