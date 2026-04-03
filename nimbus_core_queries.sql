-- ============================================
-- NimbusAI Core Analysis: PostgreSQL Queries
-- ============================================

-- 1. Monthly Churn Rate by Plan Tier
WITH MonthlyActive AS (
    SELECT 
        DATE_TRUNC('month', s.start_date) as month,
        p.plan_tier,
        COUNT(DISTINCT s.customer_id) as total_active
    FROM nimbus.subscriptions s
    JOIN nimbus.plans p ON s.plan_id = p.plan_id
    GROUP BY 1, 2
),
MonthlyChurn AS (
    SELECT 
        DATE_TRUNC('month', churned_at) as month,
        p.plan_tier,
        COUNT(DISTINCT c.customer_id) as churn_count
    FROM nimbus.customers c
    JOIN nimbus.subscriptions s ON c.customer_id = s.customer_id
    JOIN nimbus.plans p ON s.plan_id = p.plan_id
    WHERE c.is_active = FALSE AND c.churned_at IS NOT NULL
    GROUP BY 1, 2
)
SELECT 
    ma.month,
    ma.plan_tier,
    ma.total_active,
    COALESCE(mc.churn_count, 0) as churn_count,
    ROUND((COALESCE(mc.churn_count, 0)::NUMERIC / ma.total_active) * 100, 2) as churn_rate_pct
FROM MonthlyActive ma
LEFT JOIN MonthlyChurn mc ON ma.month = mc.month AND ma.plan_tier = mc.plan_tier
ORDER BY ma.month DESC, ma.plan_tier;

-- 2. Customer Lifetime Value (LTV) Distribution
SELECT 
    c.company_size,
    ROUND(AVG(total_revenue), 2) as avg_lifetime_revenue,
    ROUND(AVG(months_active), 1) as avg_tenure_months
FROM (
    SELECT 
        customer_id,
        SUM(p.monthly_price_usd) as total_revenue,
        COUNT(DISTINCT DATE_TRUNC('month', start_date)) as months_active
    FROM nimbus.subscriptions s
    JOIN nimbus.plans p ON s.plan_id = p.plan_id
    GROUP BY 1
) rev
JOIN nimbus.customers c ON rev.customer_id = c.customer_id
GROUP BY 1
ORDER BY 2 DESC;

-- 3. Support Ticket Correlation (leading indicator check)
SELECT 
    c.is_active,
    COUNT(DISTINCT c.customer_id) as customer_count,
    ROUND(AVG(ticket_count), 2) as avg_tickets_per_cust
FROM nimbus.customers c
LEFT JOIN (
    SELECT customer_id, COUNT(*) as ticket_count 
    FROM nimbus.support_tickets 
    GROUP BY 1
) t ON c.customer_id = t.customer_id
GROUP BY 1;
