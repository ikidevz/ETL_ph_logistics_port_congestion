{{
  config(materialized='table')
}}

SELECT
    MD5(shipment_id)            AS shipment_key,
    shipment_id,
    bl_number,
    origin_port_code,
    dest_port_code,
    carrier_code,
    cargo_type,
    commodity_code,
    -- Incoterm inferred from cargo type (extend with real data when available)
    CASE cargo_type
        WHEN 'Electronics'                THEN 'CIF'
        WHEN 'Semiconductors'             THEN 'CIF'
        WHEN 'Telecommunications Equipment' THEN 'CIF'
        WHEN 'Machinery'                  THEN 'CFR'
        WHEN 'Industrial Equipment'       THEN 'CFR'
        WHEN 'Mining Equipment'           THEN 'CFR'
        WHEN 'Renewable Energy Components' THEN 'CFR'
        WHEN 'Automotive Parts'           THEN 'CFR'
        WHEN 'Vehicles'                   THEN 'CFR'
        WHEN 'Food'                       THEN 'CIF'
        WHEN 'Frozen Food'                THEN 'CIF'
        WHEN 'Beverages'                  THEN 'CIF'
        WHEN 'Agricultural Products'      THEN 'CIF'
        WHEN 'Pharmaceuticals'            THEN 'CIF'
        WHEN 'Medical Equipment'          THEN 'CIF'
        WHEN 'Chemicals'                  THEN 'CFR'
        WHEN 'Petrochemicals'             THEN 'CFR'
        WHEN 'Textiles'                   THEN 'FOB'
        WHEN 'Garments'                   THEN 'FOB'
        WHEN 'Footwear'                   THEN 'FOB'
        WHEN 'Furniture'                  THEN 'FOB'
        WHEN 'Consumer Goods'             THEN 'FOB'
        WHEN 'Household Products'         THEN 'FOB'
        WHEN 'Construction Materials'     THEN 'FOB'
        WHEN 'Steel Products'             THEN 'FOB'
        WHEN 'Paper Products'             THEN 'FOB'
        WHEN 'Plastic Resins'             THEN 'FOB'
        WHEN 'Rubber Products'            THEN 'FOB'
        WHEN 'Solar Panels'               THEN 'CIF'
        WHEN 'Batteries'                  THEN 'CIF'
        ELSE 'FOB'
    END AS incoterm,
    declared_value_usd,
    CURRENT_TIMESTAMP()         AS dbt_updated_at
FROM {{ ref('stg_shipments') }}