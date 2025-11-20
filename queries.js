// Top 5 genres by total votes and average rating
printjson(
    db.titles
        .aggregate([
            {
                $project: {
                    genres: { $split: ["$genre", ","] },
                    rating: 1,
                    votes: 1,
                },
            },
            { $unwind: "$genres" },
            {
                $group: {
                    _id: { $trim: { input: "$genres" } },
                    avg_rating: { $avg: "$rating" },
                    total_votes: { $sum: "$votes" },
                },
            },
            { $sort: { total_votes: -1 } },
            { $limit: 5 },
            {
                $project: {
                    genre: "$_id",
                    avg_rating: 1,
                    total_votes: 1,
                    _id: 0,
                },
            },
        ])
        .toArray(),
);

// Years with the most well liked movies
printjson(
    db.titles.aggregate([
        {
            $addFields: {
                release_year: {
                    $regexFind: {
                        input: "$year",
                        regex: /\d{4}/,
                    },
                },
            },
        },
        {
            $addFields: {
                release_year: { $toInt: "$release_year.match" },
            },
        },
        {
            $match: {
                rating: { $gte: 80 },
                votes: { $gte: 1000 },
            },
        },
        {
            $group: {
                _id: "$release_year",
                well_liked_count: { $sum: 1 },
            },
        },
        {
            $sort: { well_liked_count: -1 },
        },
        {
            $limit: 10,
        },
    ]),
);

// Duration ranges with average rating, total movies, total votes
printjson(
    db.titles
        .aggregate([
            {
                $addFields: {
                    duration_minutes: {
                        $cond: [
                            {
                                $regexMatch: {
                                    input: "$duration",
                                    regex: "^[0-9]+",
                                },
                            },
                            {
                                $toInt: {
                                    $arrayElemAt: [
                                        { $split: ["$duration", " "] },
                                        0,
                                    ],
                                },
                            },
                            null,
                        ],
                    },
                },
            },
            { $match: { duration_minutes: { $ne: null } } },
            {
                $addFields: {
                    duration_range: {
                        $switch: {
                            branches: [
                                {
                                    case: { $lte: ["$duration_minutes", 60] },
                                    then: "0-60 min",
                                },
                                {
                                    case: { $lte: ["$duration_minutes", 120] },
                                    then: "61-120 min",
                                },
                                {
                                    case: { $lte: ["$duration_minutes", 180] },
                                    then: "121-180 min",
                                },
                            ],
                            default: "181+ min",
                        },
                    },
                },
            },
            {
                $group: {
                    _id: "$duration_range",
                    avg_rating: { $avg: "$rating" },
                    total_movies: { $sum: 1 },
                    total_votes: { $sum: "$votes" },
                },
            },
            {
                $project: {
                    duration_range: "$_id",
                    avg_rating: 1,
                    total_movies: 1,
                    total_votes: 1,
                    _id: 0,
                },
            },
            { $sort: { duration_range: 1 } },
        ])
        .toArray(),
);
