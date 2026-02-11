use energia
show collections

// QUERY 1
//Nuclear energy, though produced by only 195 plants globally, generates a huge total capacity (~407,912 MW).
//In contrast, fossil fuels like Coal or Gas have many more plants but lower average capacity per site.
//Nuclear is therefore highly efficient and a cleaner energy solution.

db.power_plant_database_global.aggregate([
  {
    // Group by primary fuel type
    $group: {
      _id: "$primary_fuel",                  
      number_of_plants: { $sum: 1 },        
      total_capacity_mw: { $sum: "$capacity_mw" } 
    }
  },
  {
    // Round total capacity to 1 decimal place
    $project: {
      number_of_plants: 1,
      total_capacity_mw: { $round: ["$total_capacity_mw", 1] }
    }
  },
  {
    // Sort descending by total capacity
    $sort: { total_capacity_mw: -1 }
  }
])

//QUERY 2
//This query calculates total nuclear capacity per country to show global distribution.
//The U.S. leads globally, while France is the top European producer.
//This highlights priority areas for further analysis, particularly the U.S., where we will examine whether uranium resources justify this high nuclear output.

db.power_plant_database_global.aggregate([
  {
    // Keep only nuclear plants
    $match: { 
      primary_fuel: "Nuclear" 
    }
  },
  {
    // Group by country
    $group: {
      _id: "$country_long",                  // country as group key
      total_nuclear_capacity_mw: { $sum: "$capacity_mw" }  // sum capacity
    }
  },
  {
    // Round the total capacity to 2 decimals
    $project: {
      total_nuclear_capacity_mw: { $round: ["$total_nuclear_capacity_mw", 2] }
    }
  },
  {
    // Sort descending by total nuclear capacity
    $sort: { total_nuclear_capacity_mw: -1 }
  },
  {
    // Limit to top 10 countries
    $limit: 10
  }
])



// QUERY 3
//This table shows the average share of electricity from nuclear energy in 2023 for each country.
//Only countries above the global average are included.
//The top nine countries are all European, with France leading, highlighting its strong reliance on nuclear power.

// Step 1: calculate the global average first
var globalAvg = db.world_nuclear_energy_generation.aggregate([
  { $match: { Year: 2023 } },
  { $group: { _id: null, avg_share: { $avg: "$share_of_electricity_pct" } } }
]).toArray()[0].avg_share;

// Step 2: find countries above the global average
db.world_nuclear_energy_generation.aggregate([
  { $match: { Year: 2023 } },
  { $group: {
      _id: "$Entity",
      avg_nuclear_share: { $avg: "$share_of_electricity_pct" }
  }},
  { $match: { avg_nuclear_share: { $gt: globalAvg } } },
  { $project: {
      _id: 1,
      avg_nuclear_share: { $round: ["$avg_nuclear_share", 2] }
  }},
  { $sort: { avg_nuclear_share: -1 } }
])


//QUERY 4
//This analysis considers only individual countries, ignoring regional or aggregated entities.
//China leads with a 53.7 TWh increase in 2019, followed by France (40.92 TWh in 2023) and Japan (25.67 TWh in 2023).
//These figures highlight the countries with the largest recent growth in nuclear electricity production.

db.world_nuclear_energy_generation.aggregate([
  // Step 1: Keep only years > 2015 and Canada
  { $match: { Year: { $gt: 2015 }, Entity: "Canada" } },

  // Step 2: Sort by year
  { $sort: { Year: 1 } },

  // Step 3: Compute previous year's generation
  { 
    $setWindowFields: {
      partitionBy: "$Entity",
      sortBy: { Year: 1 },
      output: {
        prev_year_generation: { $shift: { by: -1, output: "$electricity_from_nuclear_twh" } }
      }
    }
  },

  // Step 4: Compute annual difference
  { 
    $addFields: { 
      generation_difference: { 
        $subtract: ["$electricity_from_nuclear_twh", "$prev_year_generation"] 
      } 
    } 
  },

  // Step 5: Keep only positive increases
  { $match: { generation_difference: { $gt: 0 } } },

  // Step 6: Find the year with maximum increase
  { 
    $group: {
      _id: "$Entity",
      max_increase: { $max: "$generation_difference" },
      docs: { $push: "$$ROOT" }
    }
  },
  { 
    $project: {
      docs: {
        $filter: {
          input: "$docs",
          as: "doc",
          cond: { $eq: ["$$doc.generation_difference", "$max_increase"] }
        }
      }
    }
  },
  { $unwind: "$docs" },
  { $replaceRoot: { newRoot: "$docs" } },

  // Step 7: Select output fields
  { 
    $project: {
      _id: 0,
      Entity: 1,
      Year: 1,
      total_nuclear_generation: "$electricity_from_nuclear_twh",
      generation_difference: 1
    }
  }
])



//QUERY 5
//In this analysis, we shifted from looking at individual countries to focusing on geographical bands of 5°×5° latitude and longitude.
//This allows us to map the global distribution of nuclear power plants and evaluate the total capacity in each area, 
//rather than just counting the number of plants.
//The results show that countries like France, South Korea, and Japan dominate certain bands due to their high 
//concentration of nuclear plants in a relatively small area.
//Meanwhile, very large countries like the United States appear frequently because their territory spans 
//many bands, spreading their capacity across multiple areas.
//This demonstrates the distinction between capacity density (high capacity in a small area) and total capacity across a large territory, 
//highlighting both concentrated and dispersed contributions to global nuclear energy production.
db.power_plant_database_global.aggregate([
  // Step 1: Filter only nuclear plants with valid latitude and longitude
  { $match: { 
      primary_fuel: "Nuclear",
      latitude: { $ne: null },
      longitude: { $ne: null }
  }},

  // Step 2: Compute latitude and longitude bands (5° intervals)
  { $addFields: {
      lat_band: { 
        $concat: [
          { $toString: { $multiply: [ { $floor: { $divide: ["$latitude", 5] } }, 5 ] } }, 
          "°–", 
          { $toString: { $add: [ { $multiply: [ { $floor: { $divide: ["$latitude", 5] } }, 5 ] }, 5 ] } },
          "°"
        ]
      },
      lon_band: { 
        $concat: [
          { $toString: { $multiply: [ { $floor: { $divide: ["$longitude", 5] } }, 5 ] } }, 
          "°–", 
          { $toString: { $add: [ { $multiply: [ { $floor: { $divide: ["$longitude", 5] } }, 5 ] }, 5 ] } },
          "°"
        ]
      }
  }},

  // Step 3: Group by latitude and longitude bands
  { $group: {
      _id: { lat_band: "$lat_band", lon_band: "$lon_band" },
      plant_count: { $sum: 1 }, // Count number of plants in the band
      total_capacity_mw: { $sum: "$capacity_mw" }, // Sum total capacity in MW
      countries_in_band: { $addToSet: "$country_long" } // Collect distinct countries
  }},

  // Step 4: Convert countries array to sorted, comma-separated string
  { $addFields: {
      countries_in_band: { $reduce: {
          input: { $sortArray: { input: "$countries_in_band", sortBy: 1 } }, // Sort alphabetically
          initialValue: "",
          in: { $concat: [ "$$value", { $cond: [ { $eq: ["$$value", ""] }, "", ", " ] }, "$$this" ] }
      }}
  }},

  // Step 5: Project the final output fields
  { $project: {
      _id: 0,
      lat_band: "$_id.lat_band",
      lon_band: "$_id.lon_band",
      plant_count: 1,
      total_capacity_mw: 1,
      countries_in_band: 1
  }},

  // Step 6: Sort by total capacity descending
  { $sort: { total_capacity_mw: -1 } }
])




// QUERY 6
// This query compares the safety of energy sources using deaths per TWh.
// By pairing nuclear with each other source, we calculate how many times more dangerous each alternative is compared to nuclear energy.
db.rates_death_from_energy_production_per_twh.aggregate([
  // Step 1: Split the dataset in two groups: nuclear and others
  { $facet: {
      nuclear: [
        { $match: { Entity: "Nuclear" } },
        { $project: { _id: 0, nuclear_deaths: "$Deaths per TWh of electricity production" } }
      ],
      others: [
        { $match: { Entity: { $ne: "Nuclear" } } },
        { $project: { _id: 0, other_source: "$Entity", other_deaths: "$Deaths per TWh of electricity production" } }
      ]
  }},

  // Step 2: Combine nuclear with each other source
  { $project: {
      combinations: {
        $map: {
          input: "$others",
          as: "o",
          in: {
            nuclear: "Nuclear",
            nuclear_deaths: { $arrayElemAt: ["$nuclear.nuclear_deaths", 0] },
            other_source: "$$o.other_source",
            other_deaths: "$$o.other_deaths",
            times_more_dangerous: {
              $divide: ["$$o.other_deaths", { $arrayElemAt: ["$nuclear.nuclear_deaths", 0] }]
            }
          }
        }
      }
  }},

  // Step 3: Unwind to get one document per comparison
  { $unwind: "$combinations" },
  { $replaceRoot: { newRoot: "$combinations" } },

  // Step 4: Sort descending by times_more_dangerous
  { $sort: { times_more_dangerous: -1 } }
])


//QUERY 7
//This query compares nuclear and solar energy by country, showing total generation (GWh) and estimated deaths.
//It highlights that nuclear often produces much more energy than solar, while the associated death rate remains comparable.
db.power_plant_database_global.aggregate([
  // Step 1: Separate nuclear and solar using $facet
  { $facet: {
      nuclear: [
        { $match: { primary_fuel: "Nuclear" } },
        { $group: {
            _id: { country: "$country", country_long: "$country_long" },
            nuclear_gwh: { $sum: "$estimated_generation_gwh_2017" }
        }},
        { $match: { nuclear_gwh: { $gt: 0 } } },
        { $addFields: {
            nuclear_deaths: { $multiply: [ { $divide: ["$nuclear_gwh", 1000] }, 0.07 ] } // replace 0.07 with actual Nuclear deaths per TWh
        }}
      ],
      solar: [
        { $match: { primary_fuel: "Solar" } },
        { $group: {
            _id: { country: "$country", country_long: "$country_long" },
            solar_gwh: { $sum: "$estimated_generation_gwh_2017" }
        }},
        { $match: { solar_gwh: { $gt: 0 } } },
        { $addFields: {
            solar_deaths: { $multiply: [ { $divide: ["$solar_gwh", 1000] }, 0.02 ] } // replace 0.02 with Solar deaths per TWh
        }}
      ]
  }},

  // Step 2: Join nuclear and solar by country
  { $project: {
      combined: {
        $map: {
          input: "$nuclear",
          as: "n",
          in: {
            $mergeObjects: [
              "$$n",
              { $arrayElemAt: [
                  { $filter: {
                      input: "$solar",
                      as: "s",
                      cond: { $eq: ["$$s._id.country", "$$n._id.country"] }
                  } },
                  0
              ]}
            ]
          }
        }
      }
  }},

  // Step 3: Unwind combined array
  { $unwind: "$combined" },
  { $replaceRoot: { newRoot: "$combined" } },

  // Step 4: Project final fields
  { $project: {
      country: "$_id.country",
      country_long: "$_id.country_long",
      nuclear_gwh: 1,
      solar_gwh: 1,
      nuclear_deaths: 1,
      solar_deaths: 1
  }},

  // Step 5: Sort by nuclear_gwh descending
  { $sort: { nuclear_gwh: -1 } }
])




//QUERY 8
//This analysis compares actual vs. estimated nuclear generation in 2017 for each plant.
//We calculated absolute and percent errors to measure discrepancies, highlighting plants with the largest deviations.
//The results show that estimations are often inaccurate, with some significant differences from real output.
db.power_plant_database_global.aggregate([
  // Step 1: Filter nuclear plants with both real and estimated generation
  { $match: {
      primary_fuel: "Nuclear",
      generation_gwh_2017: { $ne: null },
      estimated_generation_gwh_2017: { $ne: null }
  }},

  // Step 2: Calculate absolute and percent error
  { $addFields: {
      absolute_error: { $round: [
        { $abs: { $subtract: ["$generation_gwh_2017", "$estimated_generation_gwh_2017"] } },
        2
      ] },
      percent_error: { $round: [
        { $multiply: [
            { $divide: [
                { $abs: { $subtract: ["$generation_gwh_2017", "$estimated_generation_gwh_2017"] } },
                "$generation_gwh_2017"
            ] },
            100
        ] },
        2
      ] }
  }},

  // Step 3: Project final fields
  { $project: {
      _id: 0,
      country: 1,
      name: 1,
      real_generation: "$generation_gwh_2017",
      estimated_generation: "$estimated_generation_gwh_2017",
      absolute_error: 1,
      percent_error: 1
  }},

  // Step 4: Sort descending by percent_error
  { $sort: { percent_error: -1 } }
])



//QUERY 9
//In this query, we examine U.S. uranium production and its link to nuclear 
//energy generation over time. By joining mining data (production and employment) 
//with nuclear generation statistics, we see that while uranium output and employment 
//have declined, total nuclear generation remains stable. This suggests increased uranium
//imports to sustain nuclear energy production.
db.uranium_production_summary_us.aggregate([
  { $lookup: {
      from: "us_nuclear_generating_statistics_1971_2021",
      localField: "Year",
      foreignField: "YEAR",
      as: "nuclear_stats"
  }},
  { $unwind: "$nuclear_stats" },
  { $project: {
      _id: 0,
      Year: 1,
      production_million_lbs: {
          $cond: [
              { $regexMatch: { input: "$Mine production of uranium", regex: /^[0-9.]+$/ } },
              { $toDouble: "$Mine production of uranium" },
              null
          ]
      },
      employees: {
          $cond: [
              { $regexMatch: { input: "$Employment", regex: /^[0-9]+$/ } },
              { $toInt: "$Employment" },
              null
          ]
      },
      generation_mwh: "$nuclear_stats.NUCLEAR GENERATION"
  }},
  { $sort: { Year: -1 } }
])



//QUERY 10
//This query analyzes U.S. nuclear energy production and uranium costs over time. 
//It calculates the uranium needed each year (0.007 lbs per MWh) 
//and multiplies it by the yearly uranium price to estimate the total uranium cost.
db.us_nuclear_generating_statistics_1971_2021.aggregate([
  // Step 1: Join con Uranium_purchase_price_us
  { $lookup: {
      from: "uranium_purchase_price_us",
      localField: "YEAR",
      foreignField: "Delivery year",
      as: "uranium_price"
  }},
  
  // Step 2: Unwind dell'array risultante
  { $unwind: "$uranium_price" },

  // Step 3: Filtra valori nulli
  { $match: {
      "NUCLEAR GENERATION": { $ne: null },
      "uranium_price.Total purchased": { $ne: null }
  }},

  // Step 4: Calcola campi derivati
  { $project: {
      _id: 0,
      year: "$YEAR",
      nuclear_generation: "$NUCLEAR GENERATION",
      uranium_needed_pounds: { $round: [ { $multiply: ["$NUCLEAR GENERATION", 0.007] }, 2 ] },
      uranium_price_usd_per_lb: "$uranium_price.Total purchased",
      total_uranium_cost_usd: { 
          $round: [ { $multiply: [ { $multiply: ["$NUCLEAR GENERATION", 0.007] }, "$uranium_price.Total purchased" ] }, 2 ]
      }
  }},

  // Step 5: Ordina dal più recente al più vecchio
  { $sort: { year: -1 } }
])
