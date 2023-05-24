const fs = require('fs');
const { MongoClient, ServerApiVersion } = require('mongodb');

const uri = "mongodb://127.0.0.1:27017";
const databaseName = "Test";
const collectionName = "Test";

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  }
});

async function connect() {
  try {
    // Connect the client to the server (optional starting in v4.7)
    await client.connect();
    console.log('Connected to MongoDB');
  } catch (error) {
    console.error('Error connecting to MongoDB:', error);
    throw error;
  }
}

async function initialize(){
  const db = client.db(databaseName);
  const collection = db.collection(collectionName);

  // Clear the collection before inserting new documents
  await collection.deleteMany({});
  console.log('Database initialized successfully');
}

async function insertAllData() {
  try {
    // Read the TSV file, change this line to other tsv files to test other datasets
    const tsvData = fs.readFileSync('100-data.tsv', 'utf-8');

    // Split the TSV data into rows and process each row
    const rows = tsvData.split('\n');
    const documents = [];
    for (let i = 0; i < rows.length; i++) {
      const values = rows[i].split('\t');

      // Assuming the TSV has two columns: 'column1' and 'column2'
      const document = {
        column1: values[0],
        column2: values[1],
        column3: values[2],
        column4: values[3],
        column5: values[4],
        column6: values[5]
      };

      documents.push(document);
    }

    const db = client.db(databaseName);
    const collection = db.collection(collectionName);

    // Measure the start time
    const startTime = new Date();

    // Insert the documents into the collection
    const result = await collection.insertMany(documents);

    // Measure the end time
    const endTime = new Date();
    const elapsedTime = endTime - startTime; // Time difference in milliseconds
    console.log(`Insert time taken: ${elapsedTime}ms`);
    console.log(`Inserted ${result.insertedCount} rows data`);
  } catch (error) {
    console.error('Error inserting data:', error);
    throw error;
  }
}

async function updateAllData() {
  try {
    const db = client.db(databaseName);
    const collection = db.collection(collectionName);

    // Measure the start time
    const startTime = new Date();

    const result = await collection.updateMany({}, { $set: { column1: 'updatedValue' , column2: 'updatedValue2', column3: 'updatedValue3', column4: 'updatedValue4', column5: 'updatedValue5', column6: 'updatedValue6'} });

    // Measure the end time
    const endTime = new Date();
    const elapsedTime = endTime - startTime; // Time difference in milliseconds
    console.log(`Update time taken: ${elapsedTime}ms`);
    console.log(`Updated ${result.modifiedCount} rows data`);
  } catch (error) {
    console.error('Error updating data:', error);
    throw error;
  }
}

async function retrieveAllData() {
  try {
    const db = client.db(databaseName);
    const collection = db.collection(collectionName);

    // Measure the start time
    const startTime = new Date();

    const result = await collection.find().toArray();

    // Measure the end time
    const endTime = new Date();
    const elapsedTime = endTime - startTime; // Time difference in milliseconds
    console.log(`Retrieve time taken: ${elapsedTime}ms`);
    console.log(`Retrieved ${result.length} rows data`);

    //To show the retrieved data
    //console.log(JSON.stringify(result, null, 2));
  } catch (error) {
    console.error('Error retrieving data:', error);
    throw error;
  }
}

async function deleteAllData() {
  try {
    const db = client.db(databaseName);
    const collection = db.collection(collectionName);

    // Measure the start time
    const startTime = new Date();

    const result = await collection.deleteMany();

    // Measure the end time
    const endTime = new Date();
    const elapsedTime = endTime - startTime; // Time difference in milliseconds
    console.log(`Delete time taken: ${elapsedTime}ms`);
    console.log(`Deleted ${result.deletedCount} rows data`);
  } catch (error) {
    console.error('Error deleting data:', error);
    throw error;
  }
}

async function run() {
  try {
    await connect();
    await initialize();
    await insertAllData();
    await updateAllData();
    await retrieveAllData();
    await deleteAllData();
  } catch (error) {
    console.error('Error running the process:', error);
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
}

run().catch(console.dir);
