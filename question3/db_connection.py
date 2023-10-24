#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #1
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2

def connectDataBase():

    # Create a database connection object using psycopg2
    conn = psycopg2.connect(
        host="your_host",
        database="your_database",
        user="your_user",
        password="your_password"
    )
    return conn

def createCategory(cur, catId, catName):

    # Insert a category in the database
    query = "INSERT INTO Categories (category_id, name) VALUES (%s, %s);"
    cur.execute(query, (catId, catName))

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    query = "SELECT category_id FROM Categories WHERE name = %s;"
    cur.execute(query, (docCat,))
    category_id = cur.fetchone()

    if category_id:
        category_id = category_id[0]

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
        query = "INSERT INTO Documents (doc_number, text, title, num_chars, date) VALUES (%s, %s, %s, %s, %s);"
        num_chars = len(docText.replace(' ', ''))  # Calculate num_chars excluding spaces
        cur.execute(query, (docId, docText, docTitle, num_chars, docDate))



    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
        terms = [term.strip('.,?!').lower() for term in docText.split()]

        for term in terms:
    # 3.2 For each term identified, check if the term already exists in the database
            query = "SELECT term FROM Terms WHERE term = %s;"
            cur.execute(query, (term,))
            existing_term = cur.fetchone()

            if existing_term:
                # Term already exists, update the count
                query = "UPDATE Document_Term SET count = count + 1 WHERE doc_number = %s AND term = %s;"
                cur.execute(query, (docId, term))
            else:
    # 3.3 In case the term does not exist, insert it into the database
                query = "INSERT INTO Terms (term, num_chars) VALUES (%s, %s);"
                num_chars = len(term)
                cur.execute(query, (term, num_chars))

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
        query = "SELECT term, count FROM Document_Term WHERE doc_number = %s;"
        cur.execute(query, (docId,))
        terms_count = cur.fetchall()
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
        terms_count = {}
        for term in terms:
            term = term.strip('.,?!').lower()
            terms_count[term] = terms_count.get(term, 0) + 1

        for term, count in terms_count.items():   
    # 4.3 Insert the term and its corresponding count into the database
            query = "INSERT INTO Document_Term (doc_number, term, count) VALUES (%s, %s, %s);"
            cur.execute(query, (docId, term, count))

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    query = "SELECT term FROM Document_Term WHERE doc_number = %s;"
    cur.execute(query, (docId,))
    terms = cur.fetchall()

    for term in terms:
        term = term[0]
    # 1.1 For each term identified, delete its occurrences in the index for that document
        query = "DELETE FROM Document_Term WHERE doc_number = %s AND term = %s;"
        cur.execute(query, (docId, term))
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
        query = "SELECT COUNT(*) FROM Document_Term WHERE term = %s;"
        cur.execute(query, (term,))
        term_count = cur.fetchone()[0]

        if term_count == 0:
            query = "DELETE FROM Terms WHERE term = %s;"
            cur.execute(query, (term,))

    # 2 Delete the document from the database
    query = "DELETE FROM Documents WHERE doc_number = %s;"
    cur.execute(query, (docId,))

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    query = "SELECT term, ARRAY_AGG(DISTINCT doc_number || ':' || count) FROM Document_Term GROUP BY term;"
    cur.execute(query)
    index = dict(cur.fetchall())

    return index