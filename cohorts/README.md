# Better cohorts for reco

## Simplified repo structure

```
.
├── cohorts # scripts with functions and tests
├── figures # analysis figures
├── notebooks # analysis notebooks
│   └── cohort_selection.ipynb # main notebook
├── queries # for early work
└── README.md
```

## Problem

We are launching a recommendation tool that identifies vendor cohorts and
suggests performance improvements based on peer comparisons within those
cohorts.  This ad hoc approach creates two risks for our MVP rollout: Weak
statistical foundation: We haven't validated whether our cohorts actually group
similar-performing vendors together Stakeholder confidence: Without a principled
justification for cohort boundaries, leadership are questioning recommendation
validity For more details, see [RFC - Reco cohort updating](https://docs.google.com/document/d/1N57UfysXpHLFVXMAi5htYw2L8hBySiKq9frPIBlvaPk/edit?tab=t.0#heading=h.wcy3kyozl6o0)
## Current cohort rule

Our current cohort rule uses a six-level nested hierarchy (Country → City → Area
→ Price → Cuisine → Grade) that was designed primarily for explainability to
account managers rather than analytical rigor. One shortcoming of the cohort
rule is that when there are an insufficient number of chains in a cohort, the
fallback is often to a broad “All” category. For example, 8% of vendors in the
UAE are in a category with the following dimension values

- Entity: tb_ae 
- City: All 
- Area: All 
- Priciness: All 
- Cuisine: All 
- Vendor Grade: All

Why so many? Our belief is that this is due to the fallback logic. A fallback
occurs when a proposed cohort has too few vendors for reliable comparison. In
the current rule, we switch from a granular cohort to the broadest relevant
cohort with the most chain ids.

## Amendments

- we investigate adding key account sub category
- we investigate removing the minimum number of chains in a cohort requirement
- we investigate removing the heirarchy of features

## Measures

- Within cohort similarity (sum of squared deviations of mean) for KPIs
associated with recommendations. Better cohorts are similar, so we want this to
be small.  
- We want to recommend products that where they are more likely not to be
  recommended. Currently we recommend when a vendor's KPI value is below the
  median. We want this gap to the median not to be too large. Otherwise, it's
  a bit too much of a stretch. So, we measure the average distance from the
  median in terms of percentage.  
    - Originally, we measured this in KPI value. However, pandas and SQL
      calculate the median in different ways.  
    - Moved to percentage.  This looks worse for Turkey. We can rationalise
      this based on the distributions of KPIs, and that the proposed cohorting
      rule generates more, smaller cohorts.  See the notebook
      `perc_gap_investigation.ipynb` and the distributions at the bottom of
      this notebook.
    - We did not calculate the percentage rank gap for the all markets.

## Recommendations 

- We recommend adding key account sub category, removing the chain logic, and having no hierarchy
