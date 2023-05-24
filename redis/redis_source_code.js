const Redis = require('ioredis');
const fs = require('fs');

const redis = new Redis();

async function clearDatabase() {
  try {
    await redis.flushall();
    console.log('Redis initialized successfully.');
  } catch (error) {
    console.error('Error clearing Redis database:', error);
    throw error;
  }
}

async function insertAllData() {
  try {
    // Read the TSV file, change this line to other tsv files to test other datasets
    const data = fs.readFileSync('1000-data.tsv', 'utf-8');
    const rows = data.split('\n');

    let rowCount = 0;
    // Number of rows to insert in each batch
    const batchSize = 100;

    // Get start time
    const startTime = Date.now();

    for (let i = 0; i < rows.length; i += batchSize) {
      const batchRows = rows.slice(i, i + batchSize);

      // Create a new pipeline for each batch
      const pipeline = redis.pipeline();
      for (const row of batchRows) {
        const values = row.split('\t');
        const key = values[0];
        const fieldValues = {
          value1: values[0],
          value2: values[1],
          value3: values[2],
          value4: values[3],
          value5: values[4],
          value6: values[5],
        };
        // Use hmset to set multiple field-value pairs
        pipeline.hmset(key, fieldValues);
        rowCount++;
      }
      // Execute the pipeline for the current batch
      await pipeline.exec();
    }

    // Get end time and calculate time taken
    const endTime = Date.now();
    const elapsedTime = endTime - startTime;
    console.log(`Insert time taken: ${elapsedTime}ms`);
    console.log(`Inserted ${rowCount} rows of data`);
  } catch (error) {
    console.error('Error inserting data into Redis:', error);
    throw error;
  }
}

async function updateAllData() {
  try {
    const keys = await redis.keys('*');
    const pipeline = redis.pipeline();

    const startTime = Date.now();

    for (const key of keys) {
      pipeline.hmset(key, 'value1', 'updatedValue1', 'value2', 'updatedValue2', 'value3', 'updatedValue3', 'value4', 'updatedValue4', 'value5', 'updatedValue5', 'value6', 'updatedValue6');
    }

    const results = await pipeline.exec();

    const endTime = Date.now();
    const elapsedTime = endTime - startTime;
    const rowsChanged = results.filter(([, result]) => result === 'OK').length;
    console.log(`Update time taken: ${elapsedTime}ms`);
    console.log(`Updated ${rowsChanged} rows of data`);
  } catch (error) {
    console.error('Error updating data in Redis:', error);
    throw error;
  }
}

async function retrieveAllData() {
  try {
    const keys = await redis.keys('*');

    // Get start time
    const startTime = Date.now();

    const values = [];
    for (const key of keys) {
      const value = await redis.hgetall(key);
      values.push(value);
    }

    // Get end time and calculate time taken
    const endTime = Date.now();
    const elapsedTime = endTime - startTime;
    console.log(`Retrieve time taken: ${elapsedTime}ms`);
    console.log(`Retrieved ${values.length} rows data`);

    //console.log('Retrieved data:');
    //console.log(values);
  } catch (error) {
    console.error('Error retrieving data from Redis:', error);
    throw error;
  }
}

async function deleteAllData() {
  try {
    // Get start time
    const startTime = Date.now();

    const keys = await redis.keys('*');
    const deleteCount = await redis.del(...keys);

    // Get end time and calculate time taken
    const endTime = Date.now();
    const elapsedTime = endTime - startTime;
    console.log(`Delete time taken: ${elapsedTime}ms`);
    console.log(`Deleted ${deleteCount} rows data`);
  } catch (error) {
    console.error('Error deleting data from Redis:', error);
    throw error;
  }
}

async function run() {
  try {
    await clearDatabase();
    await insertAllData();
    await updateAllData();
    await retrieveAllData();
    await deleteAllData();
  } catch (error) {
    console.error('Error running the process:', error);
  } finally {
    redis.quit();
  }
}

run();