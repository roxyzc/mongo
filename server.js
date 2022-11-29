const { MongoClient } = require("mongodb");
const stream = require("stream");
require("dotenv/config");

async function main() {
  const client = new MongoClient(process.env.DATABASE_URL);
  try {
    await client.connect();
    // console.log(new Date(new Date().setFullYear(new Date().getFullYear() - 1)));
    // console.log(new Date(Date.now()));
    // await createOne(client, {
    //   name: "Infinite Views",
    //   summary: "Modern home with infinite views from the infinity pool",
    //   property_type: "House",
    //   bedrooms: 6,
    //   bathrooms: 4.5,
    //   beds: 8,
    //   datesReserved: [new Date("2021-12-31"), new Date("2022-01-01")],
    // });

    // await createMany(client, [
    // {
    //     name: "roxyzc",
    //     username: "d",
    //     age: 16,
    //     address: [
    //         {no: 3}
    //     ]
    // },
    // {
    //     name: "roxyzc",
    //     username: "b",
    //     age: 13
    // },
    //   {
    //     name: "roxyzc",
    //     dates: [
    //       new Date(new Date().setFullYear(new Date().getFullYear() - 1)),
    //       new Date(Date.now()),
    //     ],
    //     pricePerNigth: 100,
    //     specialRequests: "Late checkout",
    //     breakfastIncluded: true,
    //   },
    //   {
    //     name: "rozy",
    //     dates: [
    //       new Date(new Date().setFullYear(new Date().getFullYear() - 1)),
    //       new Date(Date.now()),
    //     ],
    //     pricePerNigth: 150,
    //     specialRequests: "Late checkout",
    //     breakfastIncluded: true,
    //   },
    // ]);

    // await findOne(client, "roxyzc");

    // await findListingsWithMinimum(client, {maximumNumberOfResult: 4});

    // await update(client, 18, {name: "manusia"});

    // await upsert(client, "rox", {name: "roxyzcccc"});

    // await updateAllListingsToHavePropertyType(client);

    // await deleteListingNameOrAge(client, "roxyzc");
    // await deleteManyListing(client);
    // await printAggregation(client, "roxyzc", 10)
    // await createReservation(
    //   client,
    //   "roxyzc@gmail.com",
    //   "roxyzc",
    //   [new Date("2021-12-31"), new Date("2022-01-01")],
    //   {
    //     pricePerNight: 100,
    //     specialRequests: "Late Checkout",
    //     breakfastIncluded: true,
    //   }
    // );
    // const coba = createReservationDocument(
    //   "rozy",
    //   [
    //     new Date(new Date().setFullYear(new Date().getFullYear() - 1)),
    //     new Date(Date.now()),
    //   ],
    //   {
    //     pricePerNigth: 150,
    //     specialRequests: "Late checkout",
    //     breakfastIncluded: true,
    //   }
    // );
    // console.log(coba);
    // await createReservation(
    //   client,
    //   "example@example.com",
    //   "Infinite Views",
    //   [new Date("2021-12-31"), new Date("2022-01-01")],
    //   {
    //     pricePerNigth: 100,
    //     specialRequests: "Late checkout",
    //     breakfastIncluded: true,
    //   }
    // );

    const pipeline = [
      {
        $match: {
          operationType: "insert",
        },
      },
    ];
    await monitorListingsUsingEventEmitter(client, 150000, pipeline);
  } catch (error) {
    console.log(error);
  } finally {
    await client.close();
  }
}

main().catch(console.error);

async function monitorListingsUsingStreamApi(
  client,
  timeInMs = 60000,
  pipeline = []
) {
  const collection = client.db("sample").collection("CobaDoang");
  const changeStream = collection.watch(pipeline);
  changeStream.stream().pipe(
    new stream.Writable({
      objectMode: true,
      write: function (doc, _, cb) {
        console.log(doc);
        cb();
      },
    })
  );
}

async function monitorListingsUsingHashNext(
  client,
  timeInMs = 60000,
  pipeline = []
) {
  const collection = client.db("sample").collection("cobaDoang");
  const changeStream = collection.watch(pipeline);

  closeChangeStream(timeInMs, changeStream);

  try {
    while (await changeStream.hashNext()) {
      console.log(await changeStream.next());
    }
  } catch (error) {
    if (changeStream.closed) {
      console.log(
        "The change stream is closed. will not wait on any more changes"
      );
    } else {
      throw error;
    }
  }
}

async function monitorListingsUsingEventEmitter(
  client,
  timeInMs = 60000,
  pipeline = []
) {
  const collection = client.db("sample").collection("cobaDoang");
  const changeStream = collection.watch(pipeline);
  changeStream.on("change", (next) => {
    console.log(next);
  });

  await closeChangeStream(timeInMs, changeStream);
}

function closeChangeStream(timeInMs = 60000, changeStream) {
  return new Promise((resolve) => {
    setTimeout(() => {
      console.log("Closing the change stream");
      changeStream.close();
      resolve();
    }, timeInMs);
  });
}

async function createReservation(
  client,
  userEmail,
  nameOfListing,
  reservationDates,
  reservationDetails
) {
  const usersCollection = client.db("sample").collection("users");
  const listingsAndReviewsCollection = client
    .db("sample")
    .collection("cobaDoang");

  const reservation = createReservationDocument(
    nameOfListing,
    reservationDates,
    reservationDetails
  );

  const session = client.startSession();
  const transactionOptions = {
    readPreference: "primary",
    readConcern: { level: "local" },
    writeConcern: { w: "majority" },
  };

  try {
    const transactionResult = await session.withTransaction(async () => {
      const usersUpdateResult = await usersCollection.updateOne(
        { email: userEmail },
        { $addToSet: { reservations: reservation } },
        { session }
      );
      console.log(
        `${usersUpdateResult.matchedCount} document(s) found in the cobaAja collection with the email address ${userEmail}`
      );
      console.log(
        `${usersUpdateResult.modifiedCount} document(s) was/were updated to include the reservation`
      );

      // console.log(reservationDates);
      const isListingReservedResults =
        await listingsAndReviewsCollection.findOne(
          {
            name: nameOfListing,
            datesReserved: { $in: reservationDates },
          },
          { session }
        );
      // console.log(isListingReservedResults);
      if (isListingReservedResults) {
        await session.abortTransaction();
        console.error(
          "This listing is already reserved for at least one of the given dates. The reservation could not be created."
        );
        console.error(
          "any operations that already occurred as part of this transaction will be rolled back."
        );
        return;
      }

      const listingsAndReviewsUpdateResults =
        await listingsAndReviewsCollection.updateOne(
          {
            name: nameOfListing,
          },
          { $addToSet: { datesReserved: { $each: reservationDates } } },
          { session }
        );
      console.log(
        `${listingsAndReviewsUpdateResults.matchedCount} document(s) found in listingsAndReviews collection with the name ${nameOfListing}`
      );
      console.log(
        `${listingsAndReviewsUpdateResults.modifiedCount} document(s) was/were updated to include tge reservation dates.`
      );
    }, transactionOptions);
    // console.log(transactionResult);
    if (transactionResult) {
      console.log("The reservation was successfully created");
    } else {
      console.log("The transaction was intentionally aborted");
    }
  } catch (error) {
    console.log(
      `The transaction was aborted due to an unexpected error: ${error}`
    );
  } finally {
    session.endSession();
  }
}

function createReservationDocument(
  nameListing,
  reservationDates,
  reservationDetails
) {
  let reservation = {
    name: nameListing,
    date: reservationDates,
  };

  for (let detail in reservationDetails) {
    reservation[detail] = reservationDetails[detail];
  }

  return reservation;
}

async function printAggregation(client, name, age) {
  const pipeline = [
    {
      $match: {
        name: name,
        age: {
          $gte: age,
        },
        address: {
          $exists: true,
        },
      },
    },
    {
      $group: {
        _id: "$address.no",
      },
    },
    {
      $sort: {
        age: -1,
      },
    },
  ];

  const aggCursor = await client
    .db("sample")
    .collection("cobaDoang")
    .aggregate(pipeline);
  await aggCursor.forEach((x) => {
    console.log(`${x._id}`);
  });
}

async function deleteManyListing(client) {
  const result = await client
    .db("sample")
    .collection("cobaDoang")
    .deleteMany({});
  console.log(`${result.deletedCount} documents was/were deleted`);
}

async function deleteListingNameOrAge(client, nameORAge) {
  const result = await client
    .db("sample")
    .collection("cobaDoang")
    .deleteOne({ $or: [{ name: nameORAge }, { age: nameORAge }] });
  console.log(`${result.deletedCount} document was/were deleted`);
}

async function updateAllListingsToHavePropertyType(client) {
  const result = await client
    .db("sample")
    .collection("cobaDoang")
    .updateMany(
      {
        property_type: { $exists: false },
      },
      { $set: { property_type: "Unknown" } }
    );

  console.log(`${result.matchedCount} document(s) matched the query criteria`);
  console.log(`${result.modifiedCount} documents was/were updated`);
}

async function upsert(client, nameORAge, updateField) {
  const result = await client
    .db("sample")
    .collection("cobaDoang")
    .updateOne(
      { $or: [{ name: nameORAge }, { age: nameORAge }] },
      { $set: updateField },
      { upsert: true }
    );
  console.log(`${result.matchedCount} document(s) matched the query criteria`);

  console.log(result);
  if (result.upsertedCount > 0) {
    console.log(`One document was inserted with the id ${result.upsertedId}`);
  } else {
    console.log(`${result.modifiedCount} document(s) was/were updated`);
  }
}

async function update(client, nameORAge, updateField) {
  const result = await client
    .db("sample")
    .collection("cobaDoang")
    .updateOne(
      { $or: [{ name: nameORAge }, { age: nameORAge }] },
      { $set: updateField }
    );
  console.log(`${result.matchedCount} document(s) matched the query criteria`);
  console.log(`${result.modifiedCount} documents was/were updated`);
}

async function findListingsWithMinimum(
  client,
  { minimumAge = 0, maximumNumberOfResult = Number.MAX_SAFE_INTEGER } = {}
) {
  const cursor = await client
    .db("sample")
    .collection("cobaDoang")
    .find({
      age: { $gte: minimumAge },
    })
    .sort({ age: -1 })
    .limit(maximumNumberOfResult);
  const result = await cursor.toArray();
  if (result.length > 0) {
    result.forEach((result, i) => {
      console.log(`${i + 1}. name: ${result.name}`);
    });
  } else {
    console.log("...");
  }
}

async function createOne(client, newListing) {
  const result = await client
    .db("sample")
    .collection("cobaDoang")
    .insertOne(newListing);
  console.log(
    `new listing created with the following id: ${result.insertedId}`
  );
}

async function createMany(client, newListings) {
  const result = await client
    .db("sample")
    .collection("cobaDoang")
    .insertMany(newListings);
  console.log(
    `${result.insertedCount} new listings created with the following id(s):`
  );
  console.log(result.insertedIds);
}

async function findOne(client, nameOfListing) {
  const result = await client
    .db("sample")
    .collection("cobaDoang")
    .findOne({ name: "roxyzc" });
  if (!result) {
    console.log(`No Listings found with the name '${nameOfListing}'`);
  } else {
    console.log(
      `Found a listing in the collection with the name '${nameOfListing}'`
    );
    console.log(result);
  }
}
