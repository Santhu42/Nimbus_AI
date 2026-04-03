const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');

const uri = 'mongodb://localhost:27017/nimbus_db';

async function importData() {
    console.log('Connecting to MongoDB...');
    await mongoose.connect(uri);
    console.log('Connected to nimbus_db.');

    const filePath = path.join(__dirname, 'nimbus_events.js');
    const content = fs.readFileSync(filePath, 'utf-8');

    // Step 1: Replace ISODate("...") with valid JSON string "$date":"..."
    let cleaned = content.replace(/ISODate\("([^"]+)"\)/g, '{"$date":"$1"}');

    // Step 2: Find all insertMany blocks
    const pattern = /db\.(\w+)\.insertMany\(\[\s*([\s\S]*?)\]\);/g;
    let match;
    const collections = {};

    while ((match = pattern.exec(cleaned)) !== null) {
        const collName = match[1];
        const arrayContent = match[2];

        // Parse the array of JS objects
        // We wrap it in [] and parse as JSON
        let jsonStr = '[' + arrayContent + ']';

        // Fix trailing commas before ] (common in JS but invalid JSON)
        jsonStr = jsonStr.replace(/,\s*\]/g, ']');

        try {
            const docs = JSON.parse(jsonStr);
            if (collections[collName]) {
                collections[collName] = collections[collName].concat(docs);
            } else {
                collections[collName] = docs;
            }
            console.log(`  Parsed ${docs.length} docs for "${collName}" (total: ${collections[collName].length})`);
        } catch (err) {
            console.error(`  JSON parse error in "${collName}": ${err.message.substring(0, 100)}`);
        }
    }

    // Step 3: Drop old collections and insert fresh data
    const db = mongoose.connection.db;
    for (const [collName, docs] of Object.entries(collections)) {
        try {
            await db.dropCollection(collName);
            console.log(`Dropped old "${collName}"`);
        } catch (e) {
            // Collection didn't exist, that's fine
        }

        // Insert in batches of 500 to avoid memory issues
        const batchSize = 500;
        let inserted = 0;
        for (let i = 0; i < docs.length; i += batchSize) {
            const batch = docs.slice(i, i + batchSize);
            await db.collection(collName).insertMany(batch);
            inserted += batch.length;
        }
        console.log(`✅ Inserted ${inserted} documents into "${collName}"`);
    }

    console.log('\n--- Import Complete ---');
    await mongoose.disconnect();
}

importData().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
