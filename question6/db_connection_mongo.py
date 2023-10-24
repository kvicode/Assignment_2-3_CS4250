#-------------------------------------------------------------------------
# AUTHOR: Kevin Vi
# FILENAME: db_connection_mongo
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient

def connectDataBase():

    # Create a database connection object using pymongo
    client = MongoClient("mongodb://your_mongodb_uri")
    db = client["your_database_name"]
    return db

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.

    terms = {}
    for term in docText.lower().split():
        term = term.strip('.,?!;()[]{}"\'')  # Remove common punctuation
        if term in terms:
            terms[term] += 1
        else:
            terms[term] = 1


    # create a list of dictionaries to include term objects.
    term_objects = [{"term": term, "count": count, "num_chars": len(term)} for term, count in terms.items()]


    #Producing a final document as a dictionary including all the required document fields
    document = {
        "doc_number": docId,
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": docCat,
        "terms": term_objects
    }


    # Insert the document
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    col.delete_one({"doc_number": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    index = {}
    documents = col.find({}, {"terms.term": 1, "title": 1})
    for doc in documents:
        for term_info in doc.get("terms", []):
            term = term_info.get("term", "")
            title = doc.get("title", "")
            count = term_info.get("count", 0)

            if term not in index:
                index[term] = {}
            index[term][title] = count

    return index