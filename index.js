import { Client } from "pg";
import { MongoClient } from "mongodb";
import pc from "picocolors";

const MONGO_URI = "mongodb://localhost:27017/imdb_db";
const PG_CONFIG = {
    host: "localhost",
    database: "imdb_db",
    user: "postgres",
    port: 5432,
};
const ITERATIONS = 5;

const pg = new Client(PG_CONFIG);
const mongo = new MongoClient(MONGO_URI);

async function measureTime(fn) {
    const start = process.hrtime.bigint();
    await fn();
    const end = process.hrtime.bigint();
    return Number(end - start) / 1e6;
}

async function benchmark(fn) {
    let totalTime = 0;
    for (let i = 0; i < ITERATIONS; i++) {
        const time = await measureTime(fn);
        totalTime += time;
        console.log(`Iteration ${i + 1}: ${time.toFixed(2)} ms`);
    }
    console.log(
        pc.yellow(`Average: ${(totalTime / ITERATIONS).toFixed(2)} ms\n`),
    );
}

async function main() {
    await pg.connect();
    console.log(pc.cyan("Total Movies"));
    console.log(pc.blue("PostgreSQL"));
    await benchmark(() =>
        pg.query(`SELECT COUNT(*) AS total_movies FROM imdb`),
    );

    await mongo.connect();
    const db = mongo.db("imdb_db");
    const collection = db.collection("titles");

    console.log(pc.green("MongoDB"));
    await benchmark(() =>
        collection.aggregate([{ $count: "total_movies" }]).toArray(),
    );

    console.log(pc.cyan("Average Rating"));
    console.log(pc.blue("PostgreSQL"));
    await benchmark(() =>
        pg.query(`SELECT AVG(rating) AS avg_rating FROM imdb`),
    );

    console.log(pc.green("MongoDB"));
    await benchmark(() =>
        collection
            .aggregate([
                { $group: { _id: null, avg_rating: { $avg: "$rating" } } },
            ])
            .toArray(),
    );

    console.log(pc.cyan("Most Weighted Rated Movies"));
    console.log(pc.blue("PostgreSQL"));
    await benchmark(() =>
        pg.query(`
        SELECT
            title,
            rating,
            votes,
            (rating * votes) AS weighted_score
        FROM imdb
        ORDER BY weighted_score DESC
        LIMIT 20;
`),
    );

    console.log(pc.green("MongoDB"));
    await benchmark(() =>
        collection
            .aggregate([
                {
                    $addFields: {
                        weighted_score: { $multiply: ["$rating", "$votes"] },
                    },
                },
                {
                    $sort: { weighted_score: -1 },
                },
                {
                    $limit: 20,
                },
                {
                    $project: {
                        _id: 0,
                        title: 1,
                        rating: 1,
                        votes: 1,
                        weighted_score: 1,
                    },
                },
            ])
            .toArray(),
    );

    console.log(pc.cyan("Maximum and Minium Rating"));
    console.log(pc.blue("PostgreSQL"));
    await benchmark(() =>
        pg.query(`
        SELECT
            MAX(rating) AS highest_rating,
            MIN(rating) AS lowest_rating
        FROM imdb;
`),
    );

    console.log(pc.green("MongoDB"));
    await benchmark(() =>
        collection
            .aggregate([
                {
                    $group: {
                        _id: null,
                        highest_rating: { $max: "$rating" },
                        lowest_rating: { $min: "$rating" },
                    },
                },
            ])
            .toArray(),
    );

    await pg.end();
    await mongo.close();
}

main();
