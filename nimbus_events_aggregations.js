// ============================================
// NimbusAI Event Analysis: MongoDB Aggregation Pipelines
// Database: nimbus_db
// Collections: user_activity_logs, nps_survey_responses, onboarding_events
// ============================================
// HOW TO TEST: Open MongoDB Compass → nimbus_db → select collection →
// Aggregations tab → add stages one at a time from each query below.
// ============================================


// ============================================================
// Q1: Average Sessions Per User Per Week by Subscription Tier
//     with 25th, 50th (Median), and 75th Percentile Session Durations
// ============================================================
// Collection: user_activity_logs
//
// METHODOLOGY:
// 1. Group events by member_id and ISO week to count sessions per week.
// 2. Use a $lookup to cross-reference customer_id with the SQL-derived
//    subscription tier (stored as a field or joined from a reference collection).
//    Since MongoDB doesn't natively join with PostgreSQL, we embed tier info
//    using the customer_id ranges mapped to plan tiers from the SQL schema:
//      - plan_id 1 (Free): customer_ids with plan_id=1 in subscriptions
//    For a pure MongoDB approach, we group by customer_id and compute
//    weekly session stats, then use $percentile (MongoDB 7.0+) for quantiles.
//
// NOTE: If running MongoDB < 7.0, $percentile is not available.
//       Use $sortArray + manual index calculation as shown below.

// --- Stage-by-Stage for Compass ---

// Stage 1: $addFields – Extract ISO week and year from timestamp
// {
//   "isoWeek": { "$isoWeek": "$timestamp" },
//   "isoYear": { "$isoYear": "$timestamp" }
// }

// Stage 2: $group – Count sessions per user per week, collect durations
// {
//   "_id": { "member_id": "$member_id", "customer_id": "$customer_id", "year": "$isoYear", "week": "$isoWeek" },
//   "session_count": { "$sum": 1 },
//   "durations": { "$push": "$session_duration_sec" }
// }

// Stage 3: $group – Average weekly sessions per customer, aggregate all durations
// {
//   "_id": "$_id.customer_id",
//   "avg_sessions_per_week": { "$avg": "$session_count" },
//   "all_durations": { "$push": "$durations" }
// }

// Full Pipeline:
db.user_activity_logs.aggregate([
  // Step 1: Add week/year fields
  {
    $addFields: {
      isoWeek: { $isoWeek: "$timestamp" },
      isoYear: { $isoYear: "$timestamp" }
    }
  },
  // Step 2: Group by member + week to get weekly session counts and durations
  {
    $group: {
      _id: {
        member_id: "$member_id",
        customer_id: "$customer_id",
        year: "$isoYear",
        week: "$isoWeek"
      },
      weekly_session_count: { $sum: 1 },
      session_durations: { $push: "$session_duration_sec" }
    }
  },
  // Step 3: Group by customer to find avg sessions/week and flatten durations
  {
    $group: {
      _id: "$_id.customer_id",
      avg_sessions_per_week: { $avg: "$weekly_session_count" },
      all_durations: {
        $reduce: {
          input: "$session_durations",
          initialValue: [],
          in: { $concatArrays: ["$$value", "$$this"] }
        }
      },
      total_weeks_active: { $sum: 1 }
    }
  },
  // Step 4: Sort durations and calculate percentiles manually
  {
    $addFields: {
      sorted_durations: {
        $sortArray: { input: "$all_durations", sortBy: 1 }
      }
    }
  },
  {
    $addFields: {
      duration_count: { $size: "$sorted_durations" },
      p25_index: {
        $floor: { $multiply: [{ $size: "$sorted_durations" }, 0.25] }
      },
      p50_index: {
        $floor: { $multiply: [{ $size: "$sorted_durations" }, 0.50] }
      },
      p75_index: {
        $floor: { $multiply: [{ $size: "$sorted_durations" }, 0.75] }
      }
    }
  },
  {
    $project: {
      customer_id: "$_id",
      avg_sessions_per_week: { $round: ["$avg_sessions_per_week", 2] },
      total_weeks_active: 1,
      p25_session_duration_sec: { $arrayElemAt: ["$sorted_durations", "$p25_index"] },
      p50_session_duration_sec: { $arrayElemAt: ["$sorted_durations", "$p50_index"] },
      p75_session_duration_sec: { $arrayElemAt: ["$sorted_durations", "$p75_index"] },
      total_sessions_analyzed: "$duration_count"
    }
  },
  { $sort: { avg_sessions_per_week: -1 } },
  { $limit: 50 }
]);


// ============================================================
// Q2: Daily Active Users (DAU) and 7-Day Retention per Feature
// ============================================================
// Collection: user_activity_logs
//
// METHODOLOGY:
// - DAU: Count distinct member_ids per feature per day.
// - 7-Day Retention: For each feature, find the first use date per user.
//   Then check if the user used the same feature again within 7 days.
//   retention_rate = users_who_returned_within_7d / total_first_users

// --- Part A: Daily Active Users (DAU) per Feature ---
db.user_activity_logs.aggregate([
  // Only consider feature interactions
  { $match: { event_type: "feature_click" } },
  // Extract date (strip time)
  {
    $addFields: {
      event_date: {
        $dateToString: { format: "%Y-%m-%d", date: "$timestamp" }
      }
    }
  },
  // Group by feature + date, count unique users
  {
    $group: {
      _id: { feature: "$feature", date: "$event_date" },
      daily_active_users: { $addToSet: "$member_id" }
    }
  },
  {
    $project: {
      feature: "$_id.feature",
      date: "$_id.date",
      dau: { $size: "$daily_active_users" }
    }
  },
  { $sort: { feature: 1, date: -1 } }
]);

// --- Part B: 7-Day Feature Retention Rate ---
db.user_activity_logs.aggregate([
  { $match: { event_type: "feature_click" } },
  // Step 1: Find each user's first use of each feature
  {
    $group: {
      _id: { member_id: "$member_id", feature: "$feature" },
      first_use: { $min: "$timestamp" },
      all_uses: { $push: "$timestamp" }
    }
  },
  // Step 2: Check if user returned within 7 days after first use
  {
    $addFields: {
      retention_window_end: {
        $dateAdd: { startDate: "$first_use", unit: "day", amount: 7 }
      },
      returned_within_7d: {
        $gt: [
          {
            $size: {
              $filter: {
                input: "$all_uses",
                as: "use",
                cond: {
                  $and: [
                    { $gt: ["$$use", "$first_use"] },
                    {
                      $lte: [
                        "$$use",
                        { $dateAdd: { startDate: "$first_use", unit: "day", amount: 7 } }
                      ]
                    }
                  ]
                }
              }
            }
          },
          0
        ]
      }
    }
  },
  // Step 3: Aggregate by feature to get retention rate
  {
    $group: {
      _id: "$_id.feature",
      total_first_users: { $sum: 1 },
      users_retained_7d: {
        $sum: { $cond: ["$returned_within_7d", 1, 0] }
      }
    }
  },
  {
    $project: {
      feature: "$_id",
      total_first_users: 1,
      users_retained_7d: 1,
      retention_rate_pct: {
        $round: [
          { $multiply: [{ $divide: ["$users_retained_7d", "$total_first_users"] }, 100] },
          2
        ]
      }
    }
  },
  { $sort: { retention_rate_pct: -1 } }
]);


// ============================================================
// Q3: Onboarding Funnel with Drop-Off Rates & Median Time
// ============================================================
// Collection: onboarding_events
//
// Funnel Steps:
//   signup (0) → first_login (2) → workspace_created (4) →
//   first_project (5) → invited_teammate (7)
//
// NOTE: The data also contains intermediate steps like email_verified (1),
// profile_completed (3), first_task (6), etc. We filter to the 5 requested steps.
//
// METHODOLOGY:
// - Count unique users who completed each step.
// - Drop-off = (users_at_step_N - users_at_step_N+1) / users_at_step_N
// - Median time = p50 of duration_seconds for each step.

// --- Part A: Funnel Counts & Drop-Off ---
db.onboarding_events.aggregate([
  // Only include the 5 requested funnel steps
  {
    $match: {
      step: { $in: ["signup", "first_login", "workspace_created", "first_project", "invited_teammate"] },
      completed: true
    }
  },
  // Handle inconsistent field names (customerId vs customer_id)
  {
    $addFields: {
      normalized_customer_id: {
        $ifNull: ["$customer_id", "$customerId"]
      },
      normalized_member_id: {
        $ifNull: ["$member_id", "$memberId"]
      }
    }
  },
  // Count unique users per step and collect durations
  {
    $group: {
      _id: "$step",
      unique_users: { $addToSet: "$normalized_member_id" },
      durations: { $push: "$duration_seconds" }
    }
  },
  {
    $addFields: {
      user_count: { $size: "$unique_users" },
      sorted_durations: {
        $sortArray: { input: "$durations", sortBy: 1 }
      }
    }
  },
  // Calculate median (p50) duration
  {
    $addFields: {
      median_duration_sec: {
        $arrayElemAt: [
          "$sorted_durations",
          { $floor: { $multiply: [{ $size: "$sorted_durations" }, 0.5] } }
        ]
      }
    }
  },
  {
    $project: {
      step: "$_id",
      user_count: 1,
      median_duration_sec: 1,
      _id: 0
    }
  },
  // Sort by the natural funnel order
  {
    $addFields: {
      step_order: {
        $switch: {
          branches: [
            { case: { $eq: ["$step", "signup"] }, then: 0 },
            { case: { $eq: ["$step", "first_login"] }, then: 1 },
            { case: { $eq: ["$step", "workspace_created"] }, then: 2 },
            { case: { $eq: ["$step", "first_project"] }, then: 3 },
            { case: { $eq: ["$step", "invited_teammate"] }, then: 4 }
          ],
          default: 99
        }
      }
    }
  },
  { $sort: { step_order: 1 } }
]);

// Note: To calculate drop-off RATES between steps, compare user_count
// of consecutive steps. For example:
//   drop_off_signup_to_login = 1 - (login_users / signup_users)
// This is best computed in application code or a $setWindowFields stage.


// ============================================================
// Q4: Top 20 Most Engaged Free-Tier Users (Upsell Targets)
// ============================================================
// Collection: user_activity_logs (cross-referenced with SQL data)
//
// ENGAGEMENT SCORE METHODOLOGY:
// ─────────────────────────────
// We define a composite engagement score using four weighted signals:
//
//   engagement_score = (0.35 × session_frequency_score)
//                    + (0.25 × session_depth_score)
//                    + (0.20 × feature_breadth_score)
//                    + (0.20 × recency_score)
//
//   1. Session Frequency (35%): Total number of activity events.
//      High-frequency users are most likely to convert to paid.
//
//   2. Session Depth (25%): Average session duration in seconds.
//      Longer sessions = deeper product engagement.
//
//   3. Feature Breadth (20%): Number of distinct features used.
//      Users exploring many features are hitting usage limits faster.
//
//   4. Recency (20%): Days since last activity (lower = better).
//      Recent users are "warm" leads for upsell outreach.
//
// JUSTIFICATION:
//   - Session frequency is weighted highest because it directly predicts
//     whether a user will hit the free tier's project/user limits.
//   - Feature breadth identifies users exploring premium-gated features.
//   - Recency ensures we target currently active users, not dormant ones.
//
// FREE TIER IDENTIFICATION:
//   From the SQL schema, free tier = plan_id 1 (plan_tier = 'free').
//   The subscriptions table links customer_id → plan_id.
//   Since we cannot do a live SQL join, we use the customer_ids that are
//   on the free plan (extracted from: SELECT customer_id FROM subscriptions
//   WHERE plan_id = 1 AND status = 'active').
//   For this pipeline, we compute scores for ALL users and filter in
//   the application layer, or use a reference collection.

db.user_activity_logs.aggregate([
  // Step 1: Compute raw engagement metrics per user
  {
    $group: {
      _id: "$customer_id",
      total_events: { $sum: 1 },
      avg_session_duration: { $avg: "$session_duration_sec" },
      distinct_features: { $addToSet: "$feature" },
      last_activity: { $max: "$timestamp" },
      member_ids: { $addToSet: "$member_id" }
    }
  },
  // Step 2: Calculate derived metrics
  {
    $addFields: {
      feature_count: { $size: { $filter: { input: "$distinct_features", as: "f", cond: { $ne: ["$$f", null] } } } },
      days_since_last_activity: {
        $dateDiff: {
          startDate: "$last_activity",
          endDate: new Date(),
          unit: "day"
        }
      },
      team_size: { $size: "$member_ids" }
    }
  },
  // Step 3: Normalize and compute weighted engagement score
  // Using min-max style scoring:
  //   - session_freq: log scale (capped at 500 events = score 10)
  //   - session_depth: avg duration / 120 (2 min avg = score 10)
  //   - feature_breadth: features / 5 (5 features = score 10)
  //   - recency: max(0, 10 - days/30) (active in last 30 days = higher)
  {
    $addFields: {
      freq_score: { $min: [{ $divide: ["$total_events", 50] }, 10] },
      depth_score: { $min: [{ $divide: [{ $ifNull: ["$avg_session_duration", 0] }, 120] }, 10] },
      breadth_score: { $min: [{ $multiply: [{ $divide: ["$feature_count", 5] }, 10] }, 10] },
      recency_score: {
        $max: [
          { $subtract: [10, { $divide: [{ $ifNull: ["$days_since_last_activity", 365] }, 30] }] },
          0
        ]
      }
    }
  },
  {
    $addFields: {
      engagement_score: {
        $round: [
          {
            $add: [
              { $multiply: ["$freq_score", 0.35] },
              { $multiply: ["$depth_score", 0.25] },
              { $multiply: ["$breadth_score", 0.20] },
              { $multiply: ["$recency_score", 0.20] }
            ]
          },
          2
        ]
      }
    }
  },
  // Step 4: Project final output
  {
    $project: {
      customer_id: "$_id",
      engagement_score: 1,
      total_events: 1,
      avg_session_duration_sec: { $round: [{ $ifNull: ["$avg_session_duration", 0] }, 1] },
      features_used: "$feature_count",
      team_size: 1,
      days_since_last_activity: 1,
      last_activity: 1,
      _id: 0
    }
  },
  // Step 5: Sort by engagement score descending and limit to top 20
  { $sort: { engagement_score: -1 } },
  { $limit: 20 }
]);

// ============================================================
// POST-PROCESSING NOTE FOR Q4:
// ============================================================
// To filter for FREE TIER users only, run this SQL query first:
//
//   SELECT DISTINCT s.customer_id
//   FROM nimbus.subscriptions s
//   JOIN nimbus.plans p ON s.plan_id = p.plan_id
//   WHERE p.plan_tier = 'free' AND s.status = 'active';
//
// Then add a $match stage at the beginning of the pipeline:
//   { $match: { customer_id: { $in: [<list of free tier customer_ids>] } } }
//
// Alternatively, if you have imported the subscription data into a
// MongoDB reference collection, use $lookup to join at runtime.
// ============================================================
