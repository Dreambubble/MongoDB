from pymongo import MongoClient
import string
import datetime
import random
import time

mongo_uri = "mongodb+srv://venks:xxxxx@venkstest01-vzepv.mongodb.net/test"
client = MongoClient(mongo_uri)

startTime = time.time()

States          = ['NJ','NY', 'CT', 'MD','MA','ME']
Cities          = ['Buffalo','Jacksonville','Carolina','New Orleans']
CompanyNames    = ['Kitchen','Animal','State', 'Tastey', 'Big','City','Fish', 'Pizza','Goat', 'Salty','Sandwich','Lazy', 'Fun']
CompanyType     = ['LLC','Inc','Company','Corporation']
Foods           = ['Pizza', 'Bar Food', 'Fast Food', 'Indian', 'Italian', 'Mexican', 'American', 'Sushi Bar', 'Vegetarian']
Sports          = ['Golf','Hockey','Football','Basketball','Baseball']

def randomString( size, letters = string.ascii_letters ):

    return "".join([random.choice(letters) for _ in range(size)] )


def makeCustomer (
    myownid,
    custName,
    addressLine, CityName, StateName,
    companyName,
    custRating,
    custSportInterest,
    custFoodInterest,
    timestamp
):

    return {
             "custid"            : myownid,
             "custname"          : custName,
             "address"           : { "addressline" : addressLine, "city" : CityName, "state" : StateName},
             "company"           : companyName,
             "title"             : "title_" + randomString(20),
             "tags"              : [custSportInterest, custFoodInterest],
             "body"              : "body_" + randomString(80),
             "custrating"        : custRating,
             "rowtimestamp"      : timestamp
           }

customerDatabase    = client["customer"]
customersCollection = customerDatabase["customers"]

bulkCustomers = customersCollection.initialize_ordered_bulk_op()

ts = datetime.datetime.now()

for i in range(1000000):
    
    customerName      = "Customer_" + str(i)
    companyName       = CompanyNames[random.randint(0, (len(CompanyNames)-1))] + ' ' + CompanyNames[random.randint(0, (len(CompanyNames)-1))]  + ' ' + CompanyType[random.randint(0, (len(CompanyType)-1))]
    custRating        = random.randint(1,5)
    addressLine       = "Address_" + randomString(5)
    CityName          = Cities[random.randint(0, (len(Cities)-1))]
    StateName         = States[random.randint(0, (len(States)-1))]
    custSportInterest = Sports[random.randint(0, (len(Sports)-1))]
    custFoodInterest  = Foods[random.randint(0, (len(Foods)-1))]
    ts = ts + datetime.timedelta(seconds = 1)

    bulkCustomers.insert(makeCustomer(i, customerName, addressLine, CityName, StateName, companyName, custRating, custSportInterest, custFoodInterest, ts))

    if (i % 10000 == 0):

       bulkCustomers.execute()
       bulkCustomers = customersCollection.initialize_ordered_bulk_op()

bulkCustomers.execute()

finishTime = time.time()

print(finishTime - startTime)
