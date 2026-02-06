import sqlite3
import sys
import os
import urllib.parse

# Export the urls of a given bucket (stored within the saved db) to several txt files

class awsExport:
    
    URLS_PER_FILE = 50000
    
    def __init__(self):
        self.initDatabase()
        
        self.bucketID = None
        self.writeDir = None
        
        self.getBucketToExport()
        
        self.initWriteDirectory()
        
        self.exportBucket()
    
    
    
    def initDatabase(self):
        self.db = "test.db"
        self.con = sqlite3.connect(self.db)
        self.cur = self.con.cursor()
    
    
    def getBucketToExport(self):
        bucketInput = input("Bucket to export:")
        
        self.cur.execute("SELECT * FROM Buckets WHERE bucket=?", [bucketInput])
        bucketExist = self.cur.fetchall()
        
        if (len(bucketExist) > 0 and bucketExist[0][1] == bucketInput):
            self.bucketID = bucketExist[0][0]
            self.bucketUrl = bucketExist[0][1]
            print("found bucket!")
        
        else:
            sys.exit(f"The bucket '{bucketInput}' was not found in the db")
    
    
    
    def initWriteDirectory(self):
        self.writeDir = input("Input the folder to write the urls:")
    
    
    
    def writeUrls(self, urlArray, page):
        directory = self.writeDir + "/"
        writeFile = f"{directory}{self.writeDir}_{page}.txt"
        
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        file = open(writeFile, "a")
        for x in urlArray:
            file.write(self.bucketUrl + urllib.parse.quote(x) + "\n")
        
        file.close()
        
        print("Wrote page: " + str(page))
    
    
    
    def exportBucket(self):
        if (self.bucketID == None):
            sys.exit("A bucket url is needed to be exported")
        
        print("preparing to export")
        
        page = 0
        pageCount = self.URLS_PER_FILE
        
        while pageCount == self.URLS_PER_FILE:
            
            offset = page * self.URLS_PER_FILE
            urlArray = []
            
            # Retrieve key list
            self.cur.execute("SELECT keyUrl FROM Keys WHERE bucketID=? LIMIT ? OFFSET ?", [self.bucketID, self.URLS_PER_FILE, offset])
            keyData = self.cur.fetchall()
            pageCount = len(keyData)

            # Write keys
            for x in keyData:
                urlArray.append(x[0])
            
            self.writeUrls(urlArray, page)
            
            page += 1
        
        print("Finished writing urls!")
        
        
awsExport()