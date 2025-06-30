**Source-to-Target Mapping (Staging to Warehouse)** between the raw source tables and the final cleaned/structured **target schema**.

| Source Table     | Source Column           | Target Table         | Target Column         | Transformation / Data Type Casting |
|------------------|-------------------------|----------------------|------------------------|-------------------------------------|
| acquisition      | acquired_object_id      | fact_company_growth  | company_nk             | VARCHAR, Direct mapping             |
| acquisition      | acquired_at             | dim_date             | date_actual            | TEXT → DATE, then join to dim_date for date_id |
| acquisition      | acquisition_id          | fact_company_growth  | acquisition_count      | INT, Count per company_nk           |
| funding_rounds   | object_id               | fact_company_growth  | company_nk             | VARCHAR, Direct mapping             |
| funding_rounds   | raised_amount_usd       | fact_company_growth  | total_funding_usd      | TEXT/NUMERIC → FLOAT, SUM per company_nk |
| ipos             | object_id               | fact_company_growth  | company_nk             | VARCHAR, Direct mapping             |
| ipos             | valuation_amount        | fact_company_growth  | ipo_valuation_usd      | TEXT/NUMERIC → FLOAT, Direct mapping |
| ipos             | raised_amount           | fact_company_growth  | ipo_raised_amount_usd  | TEXT/NUMERIC → FLOAT, Direct mapping |
| investments      | investment_id           | fact_investments     | investment_nk          | VARCHAR, Direct mapping             |
| investments      | investor_object_id      | dim_investor         | investor_nk            | VARCHAR, Direct mapping             |
| investments      | funded_object_id        | dim_company          | company_nk             | VARCHAR, Direct mapping             |
| investments      | funding_round_id        | dim_funding_round    | funding_round_nk       | VARCHAR, Join with funding_rounds   |
| investments      | created_at              | dim_date             | date_actual            | TEXT → DATE, then join to dim_date for date_id |
| investments      | investment_id           | fact_investments     | investment_id          | Surrogate key SERIAL                |
| investments      | investor_object_id      | fact_investments     | investor_id            | FK to dim_investor (via investor_nk) |
| investments      | funded_object_id        | fact_investments     | company_id             | FK to dim_company (via company_nk)  |
| funding_rounds   | funding_round_type      | dim_funding_round    | funding_type           | VARCHAR, Direct mapping             |
| funding_rounds   | funding_round_code      | dim_funding_round    | funding_code           | VARCHAR, Direct mapping             |
| funding_rounds   | is_first_round          | dim_funding_round    | is_first_round         | TEXT → BOOLEAN, convert 'True'/'False' |
| funding_rounds   | is_last_round           | dim_funding_round    | is_last_round          | TEXT → BOOLEAN, convert 'True'/'False' |
| people           | people_id               | dim_person           | person_nk              | TEXT → VARCHAR, Direct mapping      |
| people           | first_name              | dim_person           | first_name             | TEXT → VARCHAR, Direct mapping      |
| people           | last_name               | dim_person           | last_name              | TEXT → VARCHAR, Direct mapping      |
| people           | birthplace              | dim_person           | birthplace             | TEXT → VARCHAR, Direct mapping      |
| relationships    | relationship_id         | dim_relationship     | relationship_nk        | VARCHAR, Direct mapping             |
| relationships    | person_object_id        | dim_relationship     | person_nk              | FK to dim_person                    |
| relationships    | relationship_object_id  | dim_relationship     | related_company_nk     | FK to dim_company                   |
| relationships    | start_at                | dim_relationship     | start_date             | TEXT → DATE, standard ISO format    |
| relationships    | end_at                  | dim_relationship     | end_date               | TEXT → DATE, standard ISO format    |
| relationships    | is_past                 | dim_relationship     | is_past                | TEXT → BOOLEAN, convert 'True'/'False' |
| relationships    | created_at              | dim_relationship     | created_at             | TEXT → TIMESTAMP, standard ISO timestamp |
| relationships    | updated_at              | dim_relationship     | updated_at             | TEXT → TIMESTAMP, standard ISO timestamp |
| dim_date CSV     | date_actual             | dim_date             | date_actual            | TEXT → DATE                         |
| dim_date CSV     | year                    | dim_date             | year                   | TEXT → INT                          |
| dim_date CSV     | month                   | dim_date             | month                  | TEXT → INT                          |
| dim_date CSV     | month_name              | dim_date             | month_name             | TEXT → VARCHAR                      |
| dim_date CSV     | week                    | dim_date             | week                   | TEXT → INT                          |
| dim_date CSV     | quarter                 | dim_date             | quarter                | TEXT → INT                          |
| dim_date CSV     | day                     | dim_date             | day                    | TEXT → INT                          |
| dim_date CSV     | day_of_week             | dim_date             | day_of_week            | TEXT → INT                          |
| dim_date CSV     | day_name                | dim_date             | day_name               | TEXT → VARCHAR                      |
| company          | object_id               | dim_company          | company_nk             | VARCHAR, Direct mapping             |
| company          | region                  | dim_company          | region                 | TEXT → VARCHAR, Direct mapping      |
| company          | city                    | dim_company          | city                   | TEXT → VARCHAR, Direct mapping      |
| company          | country_code            | dim_company          | country_code           | TEXT → VARCHAR, Direct mapping      |
| company          | latitude                | dim_company          | latitude               | TEXT/NUMERIC → NUMERIC(9,6)         |
| company          | longitude               | dim_company          | longitude              | TEXT/NUMERIC → NUMERIC(9,6)         |
