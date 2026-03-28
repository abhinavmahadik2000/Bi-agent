# data_plan.md

# Instacart Market Basket — SQL Agent Data Plan

> **Purpose:** This document is the primary context file for an AI SQL agent writing analytical queries against the Instacart Market Basket database in DuckDB. Read this file in full before writing any query.

---

## 1. Overview

### What This Database Contains

This is the **Instacart Market Basket Analysis** dataset. It captures the grocery ordering behavior of Instacart users: which products they bought, in which orders, in what sequence, and how frequently they reordered items.

### Granularity

| Table / View | Row Granularity |
|---|---|
| `orders` | One row per order (one order per user per shopping trip) |
| `order_products_prior` | One row per product line within a prior-set order |
| `order_products_train` | One row per product line within a train-set order |
| `order_products_all` | One row per product line across both prior and train sets |
| `fact_order_lines` | One row per product line, fully denormalized (all dimensions joined) |
| `products` | One row per product |
| `aisles` | One row per aisle |
| `departments` | One row per department |

### Dataset Splits (`eval_set`)

The `orders` table contains three distinct splits identified by the `eval_set` column:

| eval_set | Meaning | Has order_products rows? |
|---|---|---|
| `'prior'` | All historical orders before the final order | ✅ Yes — in `order_products_prior` |
| `'train'` | The most recent order for training users | ✅ Yes — in `order_products_train` |
| `'test'` | The most recent order for test users | ❌ No — products are withheld |

> **Critical rule:** The `test` eval_set has no corresponding rows in any `order_products_*` table. Never join test orders to product tables expecting results.

### How Tables Relate

```
departments ──┐
              ├──► products ──► order_products_prior ──┐
aisles ────────┘                                        ├──► order_products_all ──► orders
                              order_products_train ──┘                              │
                                                                                    │
                                                                          (user_id links
                                                                           orders to each other
                                                                           for user-level analysis)
```

The view `fact_order_lines` pre-joins all of the above into a single flat table for convenience.

---

## 2. Table-by-Table Documentation

---

### `orders`

**Purpose:** The central order header table. Every shopping trip placed by every user appears here exactly once. This is the only table that contains user identity (`user_id`), timing information (`order_dow`, `order_hour_of_day`, `days_since_prior_order`), and the dataset split (`eval_set`).

| Column | Type | Description | Example Values | Nullable? |
|---|---|---|---|---|
| `order_id` | BIGINT | Primary key. Unique identifier for each order. | `1`, `2`, `3625`, `1187899` | NOT NULL |
| `user_id` | BIGINT | Identifier for the user who placed the order. Multiple orders share the same `user_id`. | `1`, `112`, `4321` | NOT NULL |
| `eval_set` | VARCHAR | Dataset split: `'prior'`, `'train'`, or `'test'`. Determines which product table (if any) holds this order's line items. | `'prior'`, `'train'`, `'test'` | NOT NULL |
| `order_number` | BIGINT | 1-based sequence number of this order for this user. `1` = the user's very first order ever. Monotonically increasing per user. | `1`, `2`, `5`, `23` | NOT NULL |
| `order_dow` | BIGINT | Day of week the order was placed. **0 = Saturday, 1 = Sunday, 2 = Monday, 3 = Tuesday, 4 = Wednesday, 5 = Thursday, 6 = Friday.** | `0`, `1`, `3`, `6` | NOT NULL |
| `order_hour_of_day` | BIGINT | Hour of day (24-hour clock) when the order was placed. | `8`, `10`, `14`, `21` | NOT NULL |
| `days_since_prior_order` | DOUBLE | Number of days elapsed since this user's immediately preceding order. **NULL for every user's first order** (`order_number = 1`). Values range from 0 to 30 (30 means "30 or more days"). | `7.0`, `14.0`, `30.0`, `NULL` | **NULLABLE** — NULL when `order_number = 1` |

**Cardinality:** ~3.4 million orders total across all eval_sets. Approximately 206,000 unique users.

**Key notes:**
- Each user has exactly **one** `train` or **one** `test` order (their most recent), plus one or more `prior` orders.
- `order_number` is scoped per user — user A's order_number 3 and user B's order_number 3 are unrelated.
- `days_since_prior_order = 30` is a **capped value** meaning "30 or more days," not exactly 30.

---

### `order_products_prior`

**Purpose:** Contains the product-level line items for every order in the `'prior'` eval_set. This is the largest table and the primary source for historical purchase behavior analysis (reorder rates, basket composition, product popularity).

| Column | Type | Description | Example Values | Nullable? |
|---|---|---|---|---|
| `order_id` | BIGINT | Foreign key → `orders.order_id`. Must match an order where `eval_set = 'prior'`. | `2`, `3`, `100` | NOT NULL |
| `product_id` | BIGINT | Foreign key → `products.product_id`. | `33120`, `28985`, `9327` | NOT NULL |
| `add_to_cart_order` | BIGINT | The sequence position in which this product was added to the cart during this order. `1` = first item added. | `1`, `2`, `5`, `12` | NOT NULL |
| `reordered` | BIGINT | `1` if this user had purchased this product in a prior order before this one; `0` if this is the first time this user bought this product. | `0`, `1` | NOT NULL |

**Cardinality:** ~32 million rows (the vast majority of all order-product data).

**Key notes:**
- The `reordered` flag is relative to the **user's own history**, not global popularity.
- `add_to_cart_order` starts at `1` for each order independently.
- There is no direct `user_id` column here — you must join to `orders` to get `user_id`.

---

### `order_products_train`

**Purpose:** Contains the product-level line items for every order in the `'train'` eval_set. Structurally identical to `order_products_prior`. These are the "ground truth" most-recent orders for training users — the orders a model would try to predict.

| Column | Type | Description | Example Values | Nullable? |
|---|---|---|---|---|
| `order_id` | BIGINT | FK → `orders.order_id` where `eval_set = 'train'`. | `1`, `36`, `52` | NOT NULL |
| `product_id` | BIGINT | FK → `products.product_id`. | `196`, `10258`, `49235` | NOT NULL |
| `add_to_cart_order` | BIGINT | Cart addition sequence position within this order. | `1`, `3`, `7` | NOT NULL |
| `reordered` | BIGINT | `1` if previously purchased by this user, `0` if not. | `0`, `1` | NOT NULL |

**Cardinality:** ~1.4 million rows (~131,000 train orders × ~10 products per order on average).

**Key notes:**
- Each `order_id` in this table appears in `orders` with `eval_set = 'train'`.
- Do **not** mix this table with `order_products_prior` without using the `order_products_all` view or explicitly labeling the source, to avoid double-counting users' purchase histories.

---

### `products`

**Purpose:** Product master/dimension table. Maps `product_id` to a human-readable product name and to its aisle and department classifications.

| Column | Type | Description | Example Values | Nullable? |
|---|---|---|---|---|
| `product_id` | BIGINT | Primary key. | `1`, `2`, `100`, `49688` | NOT NULL |
| `product_name` | VARCHAR | Full product name as listed on Instacart. | `'Bulgarian Yogurt'`, `'Organic 4% Milk Fat Whole Milk Cottage Cheese'`, `'Bag of Organic Bananas'` | NOT NULL |
| `aisle_id` | BIGINT | FK → `aisles.aisle_id`. | `61`, `104`, `83` | NOT NULL |
| `department_id` | BIGINT | FK → `departments.department_id`. | `19`, `16`, `4` | NOT NULL |

**Cardinality:** ~49,688 unique products.

---

### `aisles`

**Purpose:** Dimension table mapping `aisle_id` to a descriptive aisle name. Aisles are sub-categories within departments.

| Column | Type | Description | Example Values | Nullable? |
|---|---|---|---|---|
| `aisle_id` | BIGINT | Primary key. | `1`, `2`, `61`, `134` | NOT NULL |
| `aisle` | VARCHAR | Descriptive aisle name. | `'prepared soups salads'`, `'specialty cheeses'`, `'yogurt'`, `'fresh vegetables'` | NOT NULL |

**Cardinality:** 134 unique aisles.

---

### `departments`

**Purpose:** Dimension table mapping `department_id` to a descriptive department name. Departments are the top-level product category hierarchy.

| Column | Type | Description | Example Values | Nullable? |
|---|---|---|---|---|
| `department_id` | BIGINT | Primary key. | `1`, `4`, `16`, `21` | NOT NULL |
| `department` | VARCHAR | Descriptive department name. | `'frozen'`, `'produce'`, `'dairy eggs'`, `'snacks'`, `'beverages'` | NOT NULL |

**Cardinality:** 21 unique departments.

---

### View: `order_products_all`

**Purpose:** A `UNION ALL` of `order_products_prior` and `order_products_train`. Use this view when you want to analyze product purchases across **both** prior and train orders without writing the union yourself. Adds a `source_eval_set` column to distinguish origin.

| Column | Type | Description | Example Values | Nullable? |
|---|---|---|---|---|
| `order_id` | BIGINT | FK → `orders.order_id`. | `1`, `2`, `36` | NOT NULL |
| `product_id` | BIGINT | FK → `products.product_id`. | `196`, `33120`, `9327` | NOT NULL |
| `add_to_cart_order` | BIGINT | Cart addition sequence position. | `1`, `4`, `9` | NOT NULL |
| `reordered` | BIGINT | `1` = reordered, `0` = first time. | `0`, `1` | NOT NULL |
| `source_eval_set` | VARCHAR | Which source table this row came from. | `'prior'`, `'train'` | NOT NULL |

**Cardinality:** ~33.4 million rows (sum of prior + train).

> **Warning:** This view does NOT include `test` orders (which have no product data). It also does NOT include order metadata like `user_id` — join to `orders` for that.

---

### View: `fact_order_lines`

**Purpose:** The fully denormalized, pre-joined fact table. Combines `order_products_all` + `orders` + `products` (and implicitly makes `aisle_id` and `department_id` available). Use this as your **default starting point** for most analytical queries — it saves you from writing multi-table joins manually.

| Column | Type | Source Table | Description | Example Values | Nullable? |
|---|---|---|---|---|---|
| `order_id` | BIGINT | orders / order_products_all | Order identifier | `1`, `36`, `3625` | NOT NULL |
| `user_id` | BIGINT | orders | User identifier | `1`, `112`, `4321` | NOT NULL |
| `eval_set` | VARCHAR | orders | Dataset split of the order | `'prior'`, `'train'` | NOT NULL |
| `order_number` | BIGINT | orders | Order sequence for this user | `1`, `3`, `10` | NOT NULL |
| `order_dow` | BIGINT | orders | Day of week (0=Sat … 6=Fri) | `0`, `3`, `6` | NOT NULL |
| `order_hour_of_day` | BIGINT | orders | Hour of day (0–23) | `8`, `14`, `21` | NOT NULL |
| `days_since_prior_order` | DOUBLE | orders | Days since previous order | `7.0`, `14.0`, `NULL` | **NULLABLE** — NULL for first orders |
| `product_id` | BIGINT | products | Product identifier | `196`, `33120`, `49235` | NOT NULL |
| `add_to_cart_order` | BIGINT | order_products_all | Cart position | `1`, `5`, `12` | NOT NULL |
| `reordered` | BIGINT | order_products_all | 1=reordered, 0=new | `0`, `1` | NOT NULL |
| `source_eval_set` | VARCHAR | order_products_all | `'prior'` or `'train'` | `'prior'`, `'train'` | NOT NULL |
| `product_name` | VARCHAR | products | Full product name | `'Bag of Organic Bananas'`, `'Bulgarian Yogurt'` | NOT NULL |
| `aisle_id` | BIGINT | products | Aisle identifier | `61`, `83`, `104` | NOT NULL |
| `department_id` | BIGINT | products | Department identifier | `4`, `16`, `19` | NOT NULL |

**Cardinality:** ~33.4 million rows (same as `order_products_all` — only prior + train orders).

> **Note:** `fact_order_lines` does NOT contain `test` orders (no product data exists for them) and does NOT contain the aisle/department name strings — join to `aisles` or `departments` when you need those labels.

---

## 3. Recommended Join Paths

### 3.1 Prose Description

**Starting from product lines → order metadata:**
Join `order_products_prior` (or `order_products_all`) to `orders` on `order_id` to get `user_id`, timing, and eval_set context.

**Starting from product lines → product details:**
Join `order_products_prior` (or `order_products_all`) to `products` on `product_id` to get `product_name`, `aisle_id`, `department_id`.

**Adding aisle or department names:**
Join `products` to `aisles` on `aisle_id`, or `products` to `departments` on `department_id`.

**Full denormalized access:**
Use `fact_order_lines` directly — it already includes order metadata and product name. Then optionally join to `aisles` and/or `departments` for name labels.

**User-level analysis:**
Aggregate over `orders` (or `fact_order_lines`) grouping by `user_id`. Filter `eval_set = 'prior'` when you want only historical behavior.

---

### 3.2 SQL Snippet Examples

#### A. Product lines with order metadata (manual join)
```sql
SELECT
    o.user_id,
    o.order_id,
    o.order_number,
    o.order_dow,
    o.days_since_prior_order,
    opp.product_id,
    opp.reordered
FROM order_products_prior opp
JOIN orders o
    ON opp.order_id = o.order_id
-- Note: o.eval_set