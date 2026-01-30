import requests
from lxml import etree
from io import StringIO
import sqlite3
import random
import urllib.parse
import math
import sys

"""
Quick reference:
Marker: Specifies start point of the bucket
Prefix: Filters buckets results on input
"""

class awsGet:
    
    KEY_TABLE_FILE = "keys.sql"
    BUCKET_TABLE_FILE = "buckets.sql"
    
    def __init__(self):
        
        self.displayPage = 1
        
        testDB = input("Input bucket URL: ")
        self.startMarker = input("Marker? (Input nothing if unsure, or continuing a scrape): ")
        self.prefix = input("Prefix? (Input nothing if unsure): ")
        
        # Check if url is valid
        try:
            testBucket = self.getBucketUrl(testDB)
            self.validateBucketUrl(testBucket)
            
        except:
            sys.exit("! An error occured retrieving url data, check again if the url you inputed is correct and works")
        
        
        self.bucketID = self.setupDatabase(testDB)
        
        if self.startMarker == "":
            self.startMarker = self.getStartMarker(self.bucketID)
        
        self.iterateBucket(testDB, self.startMarker)
    
    
    
    def parseContent(self, pageContent):
        xmlParser = etree.XMLParser(encoding='utf-8', recover=True)
        baseXml = etree.parse(StringIO(pageContent), xmlParser).getroot()
        
        # https://stackoverflow.com/questions/18159221/remove-namespace-and-prefix-from-xml-in-python-using-lxml
        for elem in baseXml.getiterator():
            if not (
                isinstance(elem, etree._Comment)
                or isinstance(elem, etree._ProcessingInstruction)
            ):
                elem.tag = etree.QName(elem).localname
        
        etree.cleanup_namespaces(baseXml)
        
        return baseXml

    def getBucketUrl(self, bucketUrl, marker = "", prefix = ""):
        if bucketUrl[-1:] != "/":
            bucketUrl = bucketUrl + "/"
        
        bucketUrl = bucketUrl + "?marker=" + urllib.parse.quote(marker, "")
        xmlData = requests.get(bucketUrl).content.decode().replace(' encoding="UTF-8"', "")
        
        return self.parseContent(xmlData)
    
    
    def iterateBucket(self, bucketUrl, marker = ""):
        
        self.moreExist = "true" # check if this is the last page of the bucket
        self.lastKey = marker # record the last key for the marker usage
        self.startKeyCheck = None
        
        while self.moreExist == "true":
            
            print(f"Page {self.displayPage} = {bucketUrl} : {self.lastKey}")
            
            content = self.getBucketUrl(bucketUrl, self.lastKey, self.prefix)
            
            self.validateBucketUrl(content)
            
            self.moreExist = content[4].text

            i = 0
            for x in content:
                if x.tag == "Contents":
                    contentKey = x[0].text
                    
                    # Checks if the bucket is only 1 page, and stops if so
                    if self.startKeyCheck == None:
                        self.startKeyCheck = contentKey
                    
                    elif i == 0 and self.startKeyCheck == contentKey:
                        self.cleanDuplictes()
                        sys.exit("! This bucket only has the first page visible, ending")
                    
                    
                    self.cur.execute("INSERT INTO Keys (bucketID, keyUrl) VALUES (?,?)", [self.bucketID, contentKey])
                    
                    self.lastKey = contentKey
                    i += 1
                
            self.con.commit()
            
            self.displayPage += 1
        
        print("Completed bucket!")
        
        self.cleanDuplictes()
    
    
    # Check if bucket content result has an error
    def validateBucketUrl(self, bucketContent):
        if bucketContent.tag == "Error":
            sys.exit("! An error occured retrieving the bucket: " + bucketContent[0].text)
        
        elif bucketContent.tag != "ListBucketResult":
            sys.exit("! An unknown error occured retrieving the bucket: " + bucketContent.tag)
            
    
    # Check to see if any keys have been added to the database for this bucket
    # Return the latest key if so, else return nothing
    def getStartMarker(self, bucketID):
        self.cur.execute("SELECT COUNT(*) FROM Keys WHERE bucketID=?", [bucketID])
        
        bucketOffset = self.cur.fetchall()[0][0]
        
        if bucketOffset > 0:
            bucketOffset = bucketOffset - 1
            
            # General bucket page for logging in the console
            self.displayPage = math.floor(bucketOffset / 1000)
        
            self.cur.execute("SELECT keyUrl FROM Keys WHERE bucketID=? LIMIT 1 OFFSET ?", [bucketID, bucketOffset])
            return self.cur.fetchall()[0][0]
        
        else:
            print("No entries found, starting bucket at the root")
            return ""
        
    
    # Initialize the bucket database if it hasn't been already (create needed tables)
    # Return the bucketID, either generated if it doesn't already exist for this bucket, or the existing ID
    def setupDatabase(self, bucketUrl):
        self.db = "test.db"
        self.con = sqlite3.connect(self.db)
        self.cur = self.con.cursor()
        
        self.cur.execute(open(self.KEY_TABLE_FILE).read())
        self.cur.execute(open(self.BUCKET_TABLE_FILE).read())
        
        
        
        self.cur.execute("SELECT * FROM Buckets WHERE bucket=?", [bucketUrl])
        bucketExistCheck = self.cur.fetchall()
        
        if len(bucketExistCheck) > 0:
            if bucketExistCheck[0][1] == bucketUrl:
                print("This bucket is already in the database")
                
                return bucketExistCheck[0][0]
        
        else:
            bucketRandID = random.randint(0,999)
            
            self.cur.execute("INSERT INTO Buckets (bucketID, bucket) VALUES (?,?)", [bucketRandID, bucketUrl])
            self.con.commit()
            print(f"Added the bucket {bucketUrl} into the db")
            
            return bucketRandID
    
    
    def cleanDuplictes(self):
        dupeQuery = "DELETE FROM Keys WHERE ROWID NOT IN (SELECT MIN (ROWID) FROM Keys GROUP BY keyUrl, bucketID)"
        self.cur.execute(dupeQuery)
        self.con.commit()
        
        self.cur.execute('VACUUM "main"')
        self.con.commit()

awsGet()