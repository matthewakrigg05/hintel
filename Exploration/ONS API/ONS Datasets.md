# ONS Datasets for the Housing Intelligence Project

This file captures the ONS datasets that look most relevant for the project, along with their dataset IDs so they can be used later in the API or notebook work.

## Highest-priority datasets

### 1. House price statistics for small areas in England and Wales
- Dataset ID: `house-prices-local-authority`
- Frequency: Quarterly / annual summary depending on the view used
- Why it matters: one of the most directly relevant datasets for price trends, area comparisons and local market context
- Best use in the product: headline price trend charts, area value comparisons, investment-style insights

### 2. Index of Private Housing Rental Prices
- Dataset ID: `index-private-housing-rental-prices`
- Frequency: Monthly
- Why it matters: useful for rent trends, affordability and pressure on housing demand
- Best use in the product: rental price trend pages, affordability comparisons, “cost of living” context

### 3. Population Estimates for UK, England and Wales, Scotland and Northern Ireland
- Dataset ID: `mid-year-pop-est`
- Frequency: Annual
- Why it matters: core demographic measure for understanding how areas are changing over time
- Best use in the product: population growth summaries, area popularity signals, long-term demand context

### 4. Local authority ageing statistics, based on annual mid-year population estimates
- Dataset ID: `ageing-population-estimates`
- Frequency: Annual
- Why it matters: helps explain the age structure of an area, which can influence housing need and local character
- Best use in the product: demographic breakdowns, buyer profile context, family vs older population analysis

### 5. Local authority ageing statistics, household projections for older people
- Dataset ID: `projections-older-people-in-single-households`
- Frequency: Biennial
- Why it matters: useful for understanding household structure and likely housing demand patterns
- Best use in the product: future household trend analysis, lifecycle-based area insight

### 6. Local authority ageing statistics, net internal migration people aged 65 and over and 85 and over
- Dataset ID: `older-people-net-internal-migration`
- Frequency: Annual
- Why it matters: helps show whether an area is gaining or losing residents, which is useful for trend analysis
- Best use in the product: “moving into / out of area” insights, growth and decline indicators

### 7. Earnings and hours worked, place of work and residence by local authority
- Dataset ID: `ashe-tables-7-and-8`
- Frequency: Annual
- Why it matters: provides income information that is useful for affordability and local economic comparisons
- Best use in the product: income-based affordability context, area comparison widgets

### 8. UK Labour Market
- Dataset ID: `labour-market`
- Frequency: Regularly updated, with some series published more frequently than others
- Why it matters: gives employment and economic activity context for local areas
- Best use in the product: employment strength views, economic resilience indicators

### 9. Personal well-being estimates by local authority
- Dataset ID: `wellbeing-local-authority`
- Frequency: Annual
- Why it matters: provides a softer but valuable indicator of quality of life and local sentiment
- Best use in the product: wellbeing and lifestyle-based area summaries

### 10. Life Expectancy by Local Authority
- Dataset ID: `life-expectancy-by-local-authority`
- Frequency: Annual
- Why it matters: broad indicator of health outcomes and area quality
- Best use in the product: quality-of-area context alongside housing and demographic metrics

### 11. GDP by local authority
- Dataset ID: `gdp-by-local-authority`
- Frequency: Annual
- Why it matters: useful for seeing the economic strength of an area
- Best use in the product: economic context panels for localities

### 12. Annual GDP for England, Wales and the English regions
- Dataset ID: `regional-gdp-by-year`
- Frequency: Annual
- Why it matters: good for regional economic context and comparisons
- Best use in the product: regional dashboards and broader economic benchmarking

### 13. Quarterly GDP for England, Wales and the English regions
- Dataset ID: `regional-gdp-by-quarter`
- Frequency: Quarterly
- Why it matters: useful for tracking short-term economic changes
- Best use in the product: trend charts and more recent economic movement views

### 14. Output in the construction industry
- Dataset ID: `output-in-the-construction-industry`
- Frequency: Monthly
- Why it matters: gives a supply-side view of building activity and market momentum
- Best use in the product: construction activity context for housing market conditions

## Secondary / optional datasets

### 15. Personal well-being quarterly estimates
- Dataset ID: `wellbeing-quarterly`
- Frequency: Quarterly
- Why it matters: useful if you want a more time-sensitive well-being view
- Best use in the product: time-series wellbeing charts

### 16. Suicide registrations in England and Wales by local authority
- Dataset ID: `suicides-in-the-uk`
- Frequency: Annual / periodic release
- Why it matters: a more sensitive indicator that can be used as part of a broader area-risk perspective
- Best use in the product: optional “context” layer rather than a core housing metric

### 17. Population projections for older people
- Dataset ID: `ageing-population-projections`
- Frequency: Biennial
- Why it matters: useful for long-term demographic change and future demand scenarios
- Best use in the product: long-range planning and forecasting-style analysis

### 18. Older people economic activity
- Dataset ID: `older-people-economic-activity`
- Frequency: Annual / quarterly but updated annually for consistency
- Why it matters: helpful for understanding working-age and older-age participation in the local economy
- Best use in the product: economic participation and demographic context views

## Suggested first-wave implementation
For an initial MVP, the most useful starting set would be:
1. `house-prices-local-authority`
2. `index-private-housing-rental-prices`
3. `mid-year-pop-est`
4. `ashe-tables-7-and-8`
5. `labour-market`
6. `wellbeing-local-authority`

These provide the core ingredients for pricing, affordability, demographics, employment, and area quality.
