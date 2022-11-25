const {MongoClient} = require('mongodb');
require("dotenv/config");

async function main(){
    const client = new MongoClient(process.env.DATABASE_URL);
    try {
        await client.connect();
        // await createOne(client, {
        //     name: "Lovely Loft",
        //     username: "roxyzc",
        //     age: 18
        // })

        // await createMany(client, [
        //     {
        //         name: "roxyzc",
        //         username: "d",
        //         age: 16,
        //         address: [
        //             {no: 3}
        //         ]
        //     },
        //     {
        //         name: "roxyzc",
        //         username: "b",
        //         age: 13
        //     },
        //     {
        //         name: "roxyzc",
        //         username: "c",
        //         age: 21,
        //         address: [
        //             {no: 5}
        //         ]
        //     }
        // ])

        // await findOne(client, "roxyzc");

        // await findListingsWithMinimum(client, {maximumNumberOfResult: 4});
   
        // await update(client, 18, {name: "manusia"});

        // await upsert(client, "rox", {name: "roxyzcccc"});

        // await updateAllListingsToHavePropertyType(client);

        // await deleteListingNameOrAge(client, "roxyzc");
        // await deleteManyListing(client);
        await printAggregation(client, "roxyzc", 10)
    } catch (error) {
        console.log(error);
    }
    finally{
        await client.close();
    }
}

main().catch(console.error);

async function printAggregation(client, name, age){
    const pipeline = [
        {
            '$match': {
                'name': name,
                'age': {
                    '$gte': age
                },
                'address': {
                    '$exists': true
                }
            }
        },
        {
            '$group':{
                '_id': '$address.no'
            }
        },
        {
            '$sort': {
                'age': -1
            }
        }
    ]

    const aggCursor = await client.db("sample").collection("cobaDoang").aggregate(pipeline);
    await aggCursor.forEach(x => {
        console.log(`${x._id}`);
    })
}

async function deleteManyListing(client){
    const result = await client.db("sample").collection("cobaDoang").deleteMany({});
    console.log(`${result.deletedCount} documents was/were deleted`);
}

async function deleteListingNameOrAge(client, nameORAge){
    const result = await client.db("sample").collection("cobaDoang").deleteOne({$or: [{name: nameORAge}, {age: nameORAge}]});
    console.log(`${result.deletedCount} document was/were deleted`);
}

async function updateAllListingsToHavePropertyType(client){
    const result = await client.db("sample").collection("cobaDoang").updateMany({
        property_type: {$exists: false}
    }, {$set: {property_type: "Unknown"}});

    console.log(`${result.matchedCount} document(s) matched the query criteria`);
    console.log(`${result.modifiedCount} documents was/were updated`);
}

async function upsert(client, nameORAge, updateField){
    const result = await client.db("sample").collection("cobaDoang").updateOne({$or: [
        {name: nameORAge},
        {age: nameORAge}
    ]}, {$set: updateField}, {upsert: true});
    console.log(`${result.matchedCount} document(s) matched the query criteria`);

    console.log(result);
    if(result.upsertedCount > 0){
        console.log(`One document was inserted with the id ${result.upsertedId}`);
    }else{
        console.log(`${result.modifiedCount} document(s) was/were updated`)
    }
}

async function update(client, nameORAge, updateField){
    const result = await client.db("sample").collection("cobaDoang").updateOne({$or: [
        {name: nameORAge},
        {age: nameORAge}
    ]}, {$set: updateField});
    console.log(`${result.matchedCount} document(s) matched the query criteria`);
    console.log(`${result.modifiedCount} documents was/were updated`);
}

async function findListingsWithMinimum(client, {minimumAge = 0, maximumNumberOfResult = Number.MAX_SAFE_INTEGER} = {}){
    const cursor = await client.db("sample").collection("cobaDoang").find({
        age: {$gte: minimumAge}
    }).sort({age: -1}).limit(maximumNumberOfResult);
    const result = await cursor.toArray();
    if(result.length > 0){
        result.forEach((result, i) => {
            console.log(`${i + 1}. name: ${result.name}`);
        });
    }else{
        console.log("...");
    }
}

async function createOne(client, newListing){
    const result = await client.db("sample").collection("cobaDoang").insertOne(newListing);
    console.log(`new listing created with the following id: ${result.insertedId}`);
}

async function createMany(client, newListings){
    const result = await client.db("sample").collection("cobaDoang").insertMany(newListings);
    console.log(`${result.insertedCount} new listings created with the following id(s):`);
    console.log(result.insertedIds);
}

async function findOne(client, nameOfListing){
    const result = await client.db("sample").collection("cobaDoang").findOne({name: "roxyzc"});
    if(!result){
        console.log(`No Listings found with the name '${nameOfListing}'`);
    }else{
        console.log(`Found a listing in the collection with the name '${nameOfListing}'`);
        console.log(result);
    }
}